"""Dependency-free GCE Spot guest checkpoint runner using metadata and GCS APIs."""

GCE_SPOT_RUNNER = r'''import base64, hashlib, json, os, pathlib, signal, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
bucket, prefix, job_id, attempt, checkpoint_dir, resume_dir, resume_key, command_b64, context_json, deadline_s, poll_s = sys.argv[1:]
command = base64.b64decode(command_b64).decode()
context = json.loads(context_json)
root = pathlib.Path(checkpoint_dir)
resume = pathlib.Path(resume_dir)
root.mkdir(parents=True, exist_ok=True); resume.mkdir(parents=True, exist_ok=True)
def token():
    req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token', headers={'Metadata-Flavor':'Google'})
    with urllib.request.urlopen(req, timeout=3) as response: return json.load(response)['access_token']
def gcs_url(name, upload=False):
    quoted = urllib.parse.quote(name, safe='')
    base = 'https://storage.googleapis.com/upload/storage/v1/b/' if upload else 'https://storage.googleapis.com/download/storage/v1/b/'
    # Create-only writes make the step objects and final manifest immutable.
    # If an attempt reuses a name, GCS rejects it instead of replacing evidence.
    suffix = '/o?uploadType=resumable&ifGenerationMatch=0&name=' + quoted if upload else '/o/' + quoted + '?alt=media'
    return base + urllib.parse.quote(bucket, safe='') + suffix
def get_object(name):
    req = urllib.request.Request(gcs_url(name), headers={'Authorization':'Bearer '+token()})
    with urllib.request.urlopen(req, timeout=20) as response: return response.read()
def upload_file(name, path):
    size = path.stat().st_size
    req = urllib.request.Request(gcs_url(name, True), data=b'{}', method='POST', headers={
        'Authorization':'Bearer '+token(), 'Content-Type':'application/json',
        'X-Upload-Content-Type':'application/octet-stream', 'X-Upload-Content-Length':str(size)})
    with urllib.request.urlopen(req, timeout=20) as response: location = response.headers['Location']
    with path.open('rb') as stream:
        start = 0
        while start < size:
            chunk = stream.read(8 * 1024 * 1024)
            end = start + len(chunk) - 1
            put = urllib.request.Request(location, data=chunk, method='PUT', headers={
                'Authorization':'Bearer '+token(), 'Content-Length':str(len(chunk)),
                'Content-Range':f'bytes {start}-{end}/{size}'})
            try:
                with urllib.request.urlopen(put, timeout=25): pass
            except urllib.error.HTTPError as response:
                if response.code != 308: raise
            start = end + 1
    if size == 0:
        put = urllib.request.Request(location, data=b'', method='PUT', headers={
            'Authorization':'Bearer '+token(), 'Content-Length':'0', 'Content-Range':'bytes */0'})
        with urllib.request.urlopen(put, timeout=25): pass
def safe_path(base, name):
    path = pathlib.PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or not path.parts: raise RuntimeError('unsafe checkpoint path')
    return base.joinpath(*path.parts)
if resume_key:
    manifest = json.loads(get_object(resume_key.rstrip('/')+'/MANIFEST.json'))
    if manifest.get('schema') != 'gpu-broker.gcs-checkpoint/v1' or manifest.get('job_id') != job_id or manifest.get('context') != context:
        raise RuntimeError('checkpoint identity or workload context mismatch')
    for item in manifest['files']:
        name = item['name']; body = get_object(resume_key.rstrip('/')+'/'+name)
        if len(body) != item['bytes'] or hashlib.sha256(body).hexdigest() != item['sha256']:
            raise RuntimeError('checkpoint artifact integrity mismatch: '+name)
        path = safe_path(resume, name); path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
os.environ.update({'GPU_BROKER_CHECKPOINT_DIR':str(root), 'GPU_BROKER_RESUME_DIR':str(resume),
                    'GPU_BROKER_JOB_ID':job_id, 'GPU_BROKER_ATTEMPT':attempt})
child = subprocess.Popen(command, shell=True, executable='/bin/bash', start_new_session=True, env=os.environ.copy())
def preempted():
    try:
        req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/instance/preempted', headers={'Metadata-Flavor':'Google'})
        with urllib.request.urlopen(req, timeout=2) as response: return response.read().strip() == b'TRUE'
    except Exception: return False
def checkpoint_upload():
    marker = root/'CHECKPOINT_COMPLETE'
    step = int(marker.read_text().strip())
    base = f'{prefix.rstrip("/")}/{job_id}/step-{step:012d}'
    artifacts = []
    for path in sorted(root.rglob('*')):
        if not path.is_file() or path == marker: continue
        name = path.relative_to(root).as_posix(); body_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        upload_file(base+'/'+name, path)
        artifacts.append({'name':name,'bytes':path.stat().st_size,'sha256':body_hash})
    manifest = {'schema':'gpu-broker.gcs-checkpoint/v1','job_id':job_id,'attempt':int(attempt),
                'step':step,'written_at':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                'files':artifacts,'bytes':sum(item['bytes'] for item in artifacts),'context':context}
    temporary = pathlib.Path('/tmp/gpu-broker-checkpoint-manifest.json')
    temporary.write_text(json.dumps(manifest,sort_keys=True))
    upload_file(base+'/MANIFEST.json',temporary)
    print('gpu-broker: complete GCS checkpoint published at '+base, flush=True)
while child.poll() is None:
    if preempted():
        print('gpu-broker: GCE Spot preemption notice; requesting checkpoint', flush=True)
        try: os.killpg(child.pid, signal.SIGUSR1)
        except ProcessLookupError: pass
        deadline = time.monotonic()+float(deadline_s)
        marker = root/'CHECKPOINT_COMPLETE'
        while not marker.is_file() and child.poll() is None and time.monotonic() < deadline: time.sleep(.25)
        if marker.is_file(): checkpoint_upload()
        else: print('gpu-broker: checkpoint deadline elapsed; no complete manifest published', flush=True)
        try: os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        try: child.wait(timeout=2)
        except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL)
        sys.exit(0)
    time.sleep(float(poll_s))
sys.exit(child.wait())
'''


def gce_spot_job_script(*, command: str, bucket: str, prefix: str, job_id: str,
                        attempt: int, resume_key: str, checkpoint_dir: str,
                        resume_dir: str, checkpoint_deadline_seconds: int,
                        workload_revision: str, model_revision: str | None,
                        configuration_sha256: str, poll_seconds: float = 1.0) -> str:
    """Build a guest script; it only publishes complete, hash-bound manifests."""
    import base64
    import json
    import shlex

    context = {"workload_revision": workload_revision,
               "model_revision": model_revision,
               "configuration_sha256": configuration_sha256}
    args = [bucket, prefix, job_id, str(attempt), checkpoint_dir, resume_dir,
            resume_key, base64.b64encode(command.encode()).decode(),
            json.dumps(context, sort_keys=True), str(checkpoint_deadline_seconds),
            str(poll_seconds)]
    return "python3 -c " + shlex.quote(GCE_SPOT_RUNNER) + " " + " ".join(map(shlex.quote, args))

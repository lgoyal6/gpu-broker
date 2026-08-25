# Deploying gpu-broker for a club

Two processes, one database, and a handful of things to create in AWS. Budget an
afternoon for the first setup and about ten minutes for each machine after that.

Nothing below is required to *try* it - the [quickstart](../README.md#quickstart)
runs against a simulator with no account at all. Do that first, then come back.

---

## 0. The shape

```
                    ┌──────────────┐
  members ────────▶ │  gpu web     │  reachable from the internet
                    │  no keys     │  (GitHub OAuth callback lands here)
                    └──────┬───────┘
                           │  reads and writes the queue
                    ┌──────▼───────┐
                    │  SQLite      │  one file, WAL
                    └──────▲───────┘
                           │
                    ┌──────┴───────┐
  you ────────────▶ │  gpu run     │  holds AWS keys + the lab SSH key
                    │  the daemon  │  launches and terminates everything
                    └──────────────┘
```

Both processes need the same `GPU_BROKER_HOME`. They can be the same box; the
point of the split is that the web app **cannot** reach a machine even if it is
compromised, so put them on the same host only if you are comfortable with that.

Run `gpu doctor` after every step below. It is the fastest way to find out what
you have not done yet.

---

## 1. The database

```bash
export GPU_BROKER_HOME=/var/lib/gpu-broker
gpu doctor
```

Creates `$GPU_BROKER_HOME/broker.sqlite3` and applies migrations. Back this file
up - it is the queue, the ledger, and every utilization sample. WAL mode means
you want `broker.sqlite3`, `-wal`, and `-shm` together, or use
`sqlite3 broker.sqlite3 ".backup out.db"`.

---

## 2. AWS

### 2.1 The instance profile

Every instance the broker launches gets this. It needs to register with SSM and
ship its output to CloudWatch:

```bash
aws iam create-role --role-name gpu-broker-node \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{
     "Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},
     "Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name gpu-broker-node \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

aws iam create-instance-profile --instance-profile-name gpu-broker-node
aws iam add-role-to-instance-profile \
  --instance-profile-name gpu-broker-node --role-name gpu-broker-node
```

Then attach an inline policy allowing `logs:CreateLogStream`,
`logs:PutLogEvents`, and - if you are using checkpoints - `s3:PutObject` and
`s3:GetObject` on your checkpoint bucket, plus `ecr:GetAuthorizationToken`,
`ecr:BatchGetImage`, `ecr:PutImage` and friends if you are using environments.

### 2.2 The broker's own identity

Whatever `gpu run` authenticates as needs:

```
ec2:RunInstances  ec2:CreateTags  ec2:DescribeInstances  ec2:DescribeVolumes
ec2:TerminateInstances  ec2:DescribeSpotPriceHistory
ssm:SendCommand  ssm:GetCommandInvocation  ssm:DescribeInstanceInformation
ssm:CancelCommand
logs:GetLogEvents
pricing:GetProducts            (for `gpu prices --refresh`)
ce:GetCostAndUsage             (for the report's before-column)
servicequotas:GetServiceQuota  (so `gpu doctor` can see your GPU quota)
iam:PassRole                   on gpu-broker-node
```

**`iam:PassRole` is the one everybody forgets.** Without it every launch fails
with `UnauthorizedOperation` and nothing says why. `gpu doctor` calls it out.

### 2.3 The AMI

Any GPU AMI with the SSM agent. The Deep Learning AMI and Amazon Linux 2 both
have it; a bare Ubuntu image does not. AMI ids are per-region.

### 2.4 Quota

```bash
gpu doctor   # reads L-DB2E81BA, the G/VT on-demand vCPU quota
```

**It is zero on a new account** and an increase takes a day or two. Request it
before you need it. Until then the local pool still works.

### 2.5 Config

`$GPU_BROKER_HOME/config.json`:

```json
{
  "backends": ["ec2", "local"],
  "placement_order": ["local", "spot", "ondemand"],
  "pool_budget_usd": 500.00,
  "default_budget_usd": 25.00,
  "aws": {
    "region": "us-west-2",
    "ami": "ami-0123456789abcdef0",
    "instance_profile": "gpu-broker-node",
    "log_group": "/gpu-broker/jobs",
    "max_instances": { "a10g": 4, "t4": 4 }
  }
}
```

```bash
gpu doctor            # should be green for aws credentials, ami, profile, quota
gpu tick --dry-run    # a real run_instances(DryRun=True) against your account
```

`gpu tick --dry-run` is the last thing to run before the first real launch. It
validates IAM and every parameter without starting anything.

---

## 3. Spot and checkpoints

Spot needs somewhere off-instance for checkpoints, because a preempted job comes
back on a **different machine**. Config refuses to start otherwise.

```bash
aws s3 mb s3://ucsd-club-checkpoints --region us-west-2
```

```json
{
  "backends": ["ec2", "ec2-spot", "local"],
  "checkpoint_store": "s3",
  "checkpoint_bucket": "ucsd-club-checkpoints"
}
```

Tell members to make their jobs resumable - see
[Making your job resumable](DESIGN.md#making-your-job-resumable). A job that
never checkpoints gets preempted, restarts from scratch, and after two of those
is pinned to on-demand automatically.

---

## 4. The lab machine

Give the broker's SSH user passwordless sudo for exactly two binaries:

```
# /etc/sudoers.d/gpu-broker
broker ALL=(root) NOPASSWD: /usr/bin/systemd-run, /usr/bin/systemctl
```

The job itself does **not** run as root - the broker sudoes to *create* the unit
and passes `--uid`, so training scripts run as the ordinary SSH user.

Make sure the GPU is not in `Prohibited` compute mode (MPS cannot open it):

```bash
sudo nvidia-smi -i 0 -c DEFAULT
```

```json
{
  "backends": ["local"],
  "local": {
    "hosts": [{ "hostname": "lab1.ucsd.edu", "username": "broker" }],
    "max_jobs_per_gpu": 2
  }
}
```

```bash
gpu hosts    # HEALTHY, with limits "yes"
```

If `limits` shows **NO**, the host is drained and says why. That check is not
cosmetic: `systemd-run` exits zero on a machine where the memory controller is
not delegated and applies nothing, so the broker verifies by running a throwaway
scope with a known cap and reading its own cgroup back.

---

## 5. Persistent data

Every job gets `/data`, and it is the same `/data` next time. On EC2 that is EFS.

Create the filesystem, a mount target in each subnet the instances use, and a
security group rule allowing NFS (2049) from the instances.

```json
{ "volumes": { "enabled": true, "efs_id": "fs-0123456789abcdef0" } }
```

About $0.30/GB-month. Without it a 100GB dataset downloads again every run -
three times for a job preempted twice.

---

## 6. The web app

Register an OAuth app at <https://github.com/settings/developers>. The callback
URL is `https://your-host/auth/callback`.

```json
{
  "web": {
    "base_url": "https://gpu.ucsd.edu",
    "github_client_id": "Iv1.abc123",
    "github_org": "ucsd-aws-club",
    "admins": ["your-github-login"]
  }
}
```

Secrets come from the environment, never the config file:

```bash
export GPU_BROKER_GITHUB_CLIENT_SECRET=...
export GPU_BROKER_SESSION_SECRET=$(openssl rand -base64 32)   # or sessions drop on restart
export GPU_BROKER_METRICS_TOKEN=$(openssl rand -hex 16)       # /metrics is off without this
```

Membership is by GitHub org **or** an allowlist (`web.allowlist`,
`web.allowlist_file`), checked on every sign-in - somebody who leaves the org
stops being able to spend at their next visit rather than never.

Put a TLS terminator in front of it. The app speaks plain HTTP.

---

## 7. Running both

```ini
# /etc/systemd/system/gpu-broker-daemon.service
[Unit]
Description=gpu-broker scheduler
After=network-online.target

[Service]
User=broker
Environment=GPU_BROKER_HOME=/var/lib/gpu-broker
ExecStart=/opt/gpu-broker/.venv/bin/gpu run --interval 30
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/gpu-broker-web.service
[Unit]
Description=gpu-broker web
After=network-online.target

[Service]
User=broker-web
Environment=GPU_BROKER_HOME=/var/lib/gpu-broker
EnvironmentFile=/etc/gpu-broker/web.env
ExecStart=/opt/gpu-broker/.venv/bin/gpu web --host 127.0.0.1 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

`gpu run` exits when the queue drains, which is why `Restart=always` is not
optional - it is the loop.

Weekly digest, from cron:

```
0 9 * * MON  GPU_BROKER_HOME=/var/lib/gpu-broker /opt/gpu-broker/.venv/bin/gpu digest --send
```

with `"digest_webhook"` set to a Slack or Discord incoming webhook. Without one,
`gpu digest` still prints and you paste it wherever the club actually talks.

---

## 8. Before you tell anyone

```bash
gpu doctor                 # everything green
gpu tick --dry-run         # real DryRun against your account
gpu reap                   # should be clean
gpu report --baseline      # pulls pre-broker spend from Cost Explorer
```

That last one matters. The baseline can only be captured *before* the broker
starts changing behaviour, and Cost Explorer keeps about 13 months.

Then run one real job yourself, end to end, and read `gpu status` on it. Watch
`gpu reclaim` for a couple of weeks before setting `reclaim_enabled` - it is off
by default so you can see it be right before it is allowed to be wrong.

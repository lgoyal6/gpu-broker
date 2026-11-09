from gpu_broker.gcp import GCE_SPOT_RUNNER


def test_checkpoint_uploads_are_create_only_and_manifest_is_last():
    assert "ifGenerationMatch=0" in GCE_SPOT_RUNNER
    artifact_upload = GCE_SPOT_RUNNER.index("upload_file(base+'/'+name, path)")
    manifest_upload = GCE_SPOT_RUNNER.index("upload_file(base+'/MANIFEST.json',temporary)")
    assert artifact_upload < manifest_upload

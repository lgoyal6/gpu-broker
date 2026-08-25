"""Named environments: the digest, the cache, and where each one gets built.

The digest is the whole design. It is the cache key everywhere, so the two
things worth being sure of are that the same spec always produces the same key
and that a different spec never produces the same one. The first makes reuse
work; the second is what makes reuse *safe*.
"""

from __future__ import annotations

import pytest

from gpu_broker.environments import (
    DEFAULT_BASE,
    Build,
    Environment,
    EnvironmentError_,
    from_requirements,
)
from gpu_broker.errors import BrokerError
from gpu_broker.volumes import (
    DATA_PATH,
    VolumeConfig,
    efs_mount_script,
    local_data_dir,
    local_mount_script,
)


# ------------------------------------------------------------------ the digest


def test_the_same_spec_gives_the_same_digest():
    assert Environment("a", requirements=("torch",)).digest == Environment(
        "b", requirements=("torch",)
    ).digest


def test_reordering_requirements_is_not_a_rebuild():
    """A member who tidies their requirements.txt should not wait twenty
    minutes for the same environment to be built again."""
    one = Environment("e", requirements=("torch==2.3.0", "numpy", "pillow"))
    two = Environment("e", requirements=("pillow", "numpy", "torch==2.3.0"))
    assert one.digest == two.digest


def test_whitespace_and_case_do_not_change_the_digest():
    assert (
        Environment("e", requirements=("Torch == 2.3.0",)).digest
        == Environment("e", requirements=("torch==2.3.0",)).digest
    )


def test_changing_a_version_changes_the_digest():
    """The property that makes a cached build safe: a changed spec is a
    different key, so a stale hit is impossible."""
    assert (
        Environment("e", requirements=("torch==2.3.0",)).digest
        != Environment("e", requirements=("torch==2.4.0",)).digest
    )


def test_adding_a_package_changes_the_digest():
    assert (
        Environment("e", requirements=("torch",)).digest
        != Environment("e", requirements=("torch", "numpy")).digest
    )


def test_changing_the_base_image_changes_the_digest():
    assert (
        Environment("e", base_image="a:1").digest != Environment("e", base_image="a:2").digest
    )


def test_apt_packages_count_towards_the_digest():
    assert (
        Environment("e", apt_packages=("ffmpeg",)).digest != Environment("e").digest
    )


def test_the_name_is_not_part_of_the_digest():
    """Two people who happen to ask for the same packages should share a build."""
    assert Environment("ana-env", requirements=("torch",)).digest == Environment(
        "bo-env", requirements=("torch",)
    ).digest


def test_a_digest_is_short_enough_to_be_a_docker_tag():
    digest = Environment("e", requirements=("torch",)).digest
    assert len(digest) == 16
    assert digest.isalnum()


# ------------------------------------------------------------------ validation


@pytest.mark.parametrize("name", ["Vision", "a", "has space", "-leading", "x" * 45, ""])
def test_unusable_names_are_refused(name):
    with pytest.raises(EnvironmentError_):
        Environment(name, requirements=("torch",)).validate()


@pytest.mark.parametrize("name", ["vision", "torch-2.3", "my_env", "a1"])
def test_usable_names_are_accepted(name):
    Environment(name, requirements=("torch",)).validate()


def test_a_pip_flag_is_refused_with_the_reason():
    """`-r other.txt` refers to a file the build cannot see, and the failure
    otherwise happens twenty minutes in rather than at creation."""
    with pytest.raises(EnvironmentError_, match="cannot see"):
        Environment("vision", requirements=("-r other.txt",)).validate()


def test_an_editable_install_is_refused():
    with pytest.raises(EnvironmentError_, match="cannot see"):
        Environment("vision", requirements=("-e .",)).validate()


def test_a_nonsense_apt_package_is_refused():
    with pytest.raises(EnvironmentError_, match="apt package"):
        Environment("vision", apt_packages=("rm -rf /",)).validate()


# ------------------------------------------------------- requirements.txt path


def test_comments_and_blanks_are_stripped():
    environment = from_requirements("mine", "# deps\ntorch==2.3\n\nnumpy  # arrays\n")
    assert environment.requirements == ("torch==2.3", "numpy")


def test_an_empty_requirements_file_is_a_valid_environment():
    """The base image on its own is a perfectly good environment."""
    environment = from_requirements("bare", "# nothing here\n")
    assert environment.requirements == ()
    assert environment.empty


# --------------------------------------------------------------- the dockerfile


def test_the_dockerfile_installs_what_was_asked_for():
    body = Environment("vision", requirements=("torch==2.3.0", "numpy"),
                       apt_packages=("ffmpeg",)).dockerfile()
    assert body.startswith(f"FROM {DEFAULT_BASE}")
    assert "pip install --no-cache-dir" in body
    assert "torch==2.3.0" in body and "numpy" in body
    assert "ffmpeg" in body


def test_the_dockerfile_does_not_keep_a_pip_cache():
    """It doubles the image size and nothing reads it again."""
    assert "--no-cache-dir" in Environment("e", requirements=("torch",)).dockerfile()


def test_apt_lists_are_cleaned_up():
    body = Environment("e", apt_packages=("ffmpeg",)).dockerfile()
    assert "rm -rf /var/lib/apt/lists" in body


def test_the_image_is_labelled_with_the_environment():
    body = Environment("vision", requirements=("torch",)).dockerfile()
    assert 'gpu-broker.environment="vision"' in body


def test_a_bare_environment_needs_no_install_layer():
    assert "pip install" not in Environment("bare").dockerfile()


# --------------------------------------------------------------- the venv path


def test_the_venv_script_short_circuits_when_it_already_exists():
    """Every job runs this, and almost every job finds it already there."""
    script = Environment("e", requirements=("torch",)).venv_script("/envs")
    assert script.startswith("set -e; if [ -x /envs/")
    assert "exit 0" in script


def test_the_venv_is_built_beside_the_target_and_moved():
    """Two jobs starting at once must not see a half-installed environment."""
    environment = Environment("e", requirements=("torch",))
    script = environment.venv_script("/envs")
    assert f"/envs/.building-{environment.digest}" in script
    # Quoting is shlex's business; what matters is the staging path is moved
    # onto the final one rather than being installed into directly.
    assert script.index(f"/envs/.building-{environment.digest}") < script.index("mv ")
    assert script.rstrip().endswith(f"/envs/{environment.digest}")


def test_the_venv_path_is_keyed_on_the_digest_not_the_name():
    environment = Environment("vision", requirements=("torch",))
    assert environment.venv_path("/envs").endswith(environment.digest)


def test_a_bare_environment_still_makes_a_venv():
    assert "python3 -m venv" in Environment("bare").venv_script("/envs")


def test_a_python_version_is_honoured():
    assert "python3.11 -m venv" in Environment(
        "e", python_version="3.11"
    ).venv_script("/envs")


# ------------------------------------------------------------------- storage


def test_an_environment_round_trips(broker):
    from gpu_broker.environments import Environment as Env

    saved = broker.store.save_environment(
        Env("vision", requirements=("torch==2.3.0", "numpy"), apt_packages=("ffmpeg",),
            owner="ana", description="video work")
    )
    loaded = broker.store.environment("vision")

    assert loaded.requirements == ("torch==2.3.0", "numpy")
    assert loaded.apt_packages == ("ffmpeg",)
    assert loaded.owner == "ana"
    assert loaded.digest == saved.digest


def test_updating_an_environment_changes_its_digest(broker):
    broker.store.save_environment(Environment("vision", requirements=("torch==2.3.0",)))
    first = broker.store.environment("vision").digest
    broker.store.save_environment(Environment("vision", requirements=("torch==2.4.0",)))
    assert broker.store.environment("vision").digest != first


def test_builds_are_recorded_per_target(broker):
    """One environment can be an image on EC2 and a virtualenv on the lab box
    at the same time."""
    environment = broker.store.save_environment(Environment("v", requirements=("torch",)))
    broker.store.record_build(environment.digest, "image", Build.READY, reference="ecr/x:abc")
    broker.store.record_build(environment.digest, "venv", Build.READY, reference="/envs/abc")

    targets = {b.target for b in broker.store.builds_for(environment.digest)}
    assert targets == {"image", "venv"}
    assert broker.store.build(environment.digest, "image").usable


def test_a_failed_build_is_not_usable(broker):
    environment = broker.store.save_environment(Environment("v", requirements=("torch",)))
    broker.store.record_build(environment.digest, "image", Build.FAILED, detail="no such package")
    assert not broker.store.build(environment.digest, "image").usable


def test_an_old_build_survives_the_environment_changing(broker):
    """Jobs already running against the old digest keep working."""
    first = broker.store.save_environment(Environment("v", requirements=("torch==2.3.0",)))
    broker.store.record_build(first.digest, "image", Build.READY, reference="ecr/x:one")
    second = broker.store.save_environment(Environment("v", requirements=("torch==2.4.0",)))

    assert broker.store.build(first.digest, "image").usable
    assert broker.store.build(second.digest, "image") is None


def test_deleting_an_environment_leaves_its_builds(broker):
    environment = broker.store.save_environment(Environment("v", requirements=("torch",)))
    broker.store.record_build(environment.digest, "image", Build.READY, reference="x")
    broker.store.delete_environment("v")

    assert broker.store.environment("v") is None
    assert broker.store.build(environment.digest, "image").usable


# -------------------------------------------------------------- submitting


def test_a_job_can_name_an_environment(broker, users):
    broker.store.save_environment(Environment("vision", requirements=("torch",)))
    job = broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=1,
        environment="vision",
    ).job
    assert broker.status(job.job_id).environment == "vision"


def test_naming_an_environment_that_does_not_exist_lists_the_real_ones(broker, users):
    broker.store.save_environment(Environment("vision", requirements=("torch",)))
    with pytest.raises(BrokerError, match="Known: vision"):
        broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1, environment="nope")


def test_the_error_is_helpful_when_there_are_none_at_all(broker, users):
    with pytest.raises(BrokerError, match="None have been created yet"):
        broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1, environment="nope")


def test_a_job_with_no_environment_is_fine(broker, users):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    assert broker.status(job.job_id).environment is None


# --------------------------------------------------------------- data volumes


def test_the_efs_mount_gives_a_job_only_its_owners_data():
    """The whole filesystem is mounted, but only the user's directory is bound
    onto /data, so a job cannot see anybody else's."""
    script = efs_mount_script(VolumeConfig(enabled=True, efs_id="fs-abc"), "ana", "us-west-2")
    assert "fs-abc.efs.us-west-2.amazonaws.com" in script
    assert f"mount --bind /mnt/gpu-broker/ana {DATA_PATH}" in script


def test_the_mount_creates_the_directory_before_binding_it():
    """It cannot be mounted before it exists, and it cannot be created before
    the filesystem is mounted."""
    script = efs_mount_script(VolumeConfig(enabled=True, efs_id="fs-abc"), "ana", "us-west-2")
    assert script.index("mount -t nfs4") < script.index("mkdir -p /mnt/gpu-broker/ana")
    assert script.index("mkdir -p /mnt/gpu-broker/ana") < script.index("mount --bind")


def test_no_efs_configured_means_no_mount():
    assert efs_mount_script(VolumeConfig(enabled=True), "ana", "us-west-2") == "true"


def test_the_lab_volume_is_just_a_directory():
    config = VolumeConfig(enabled=True)
    assert local_data_dir(config, "ana", "/home/broker") == "/home/broker/.gpu-broker/data/ana"
    assert "mkdir -p" in local_mount_script(config, "ana", "/home/broker")


def test_a_login_cannot_escape_its_own_directory():
    """This is interpolated into a shell command and a mount point."""
    path = local_data_dir(VolumeConfig(enabled=True), "../../etc", "/home/broker")
    assert "/../" not in path
    assert path.startswith("/home/broker/.gpu-broker/data/")


def test_a_nonsense_efs_id_is_refused():
    from gpu_broker.errors import ConfigError

    with pytest.raises(ConfigError, match="fs-0123abc"):
        VolumeConfig(enabled=True, efs_id="my-filesystem").validate()

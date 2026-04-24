"""Tests for the `scripthut setup-aws-ec2` guided setup script."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripthut.config_schema import EC2BackendConfig, ScriptHutConfig
from scripthut.setup.aws_ec2 import (
    IAM_TEMPLATE_BODY,
    append_backend_to_yaml,
    build_backend_dict,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


def test_embedded_template_matches_disk_copy():
    """The template embedded in the Python module must match the disk copy
    at cloudformation/scripthut-ec2-iam.yaml — otherwise the docs and the
    in-process setup script will drift."""
    on_disk = (REPO_ROOT / "cloudformation" / "scripthut-ec2-iam.yaml").read_text()
    assert on_disk == IAM_TEMPLATE_BODY, (
        "Embedded IAM_TEMPLATE_BODY differs from cloudformation/scripthut-ec2-iam.yaml. "
        "Update both copies when changing the template."
    )


def test_embedded_template_parses_as_yaml():
    """Sanity: the embedded template is well-formed YAML (CFN intrinsics ignored)."""
    class IgnoreUnknown(yaml.SafeLoader):
        pass
    IgnoreUnknown.add_multi_constructor("!", lambda *a, **kw: None)
    doc = yaml.load(IAM_TEMPLATE_BODY, Loader=IgnoreUnknown)
    assert "Resources" in doc
    assert "Outputs" in doc
    assert "TaskInstanceProfile" in doc["Resources"]
    assert "InstanceProfileArn" in doc["Outputs"]


class TestBuildBackendDict:
    def test_minimal(self):
        d = build_backend_dict(
            name="aws-ec2",
            region="us-east-2",
            profile=None,
            ami="ami-1",
            subnet_id="subnet-1",
            security_group_ids=[],
            instance_profile_arn="arn:aws:iam::1:instance-profile/x",
            default_image="img:t",
            default_instance_type="c5.xlarge",
            ssh_user="ec2-user",
            max_instances=20,
        )
        assert d["type"] == "ec2"
        assert d["aws"] == {"region": "us-east-2"}  # profile omitted when None
        assert d["instance_types"] == {"default": "c5.xlarge"}
        assert d["max_instances"] == 20

    def test_includes_profile_when_set(self):
        d = build_backend_dict(
            name="aws-ec2", region="us-east-2", profile="myprof",
            ami="ami-1", subnet_id="subnet-1", security_group_ids=["sg-1"],
            instance_profile_arn="arn:x", default_image="img:t",
            default_instance_type="c5.xlarge", ssh_user="ec2-user",
            max_instances=10,
        )
        assert d["aws"] == {"region": "us-east-2", "profile": "myprof"}
        assert d["security_group_ids"] == ["sg-1"]

    def test_dict_roundtrips_through_pydantic(self):
        """The shape we write must validate as a real EC2BackendConfig."""
        d = build_backend_dict(
            name="aws-ec2", region="us-east-2", profile="p",
            ami="ami-1", subnet_id="subnet-1", security_group_ids=["sg-1", "sg-2"],
            instance_profile_arn="arn:aws:iam::1:instance-profile/x",
            default_image="img:t", default_instance_type="c5.xlarge",
            ssh_user="ec2-user", max_instances=15,
        )
        cfg = EC2BackendConfig(**d)  # raises on schema mismatch
        assert cfg.name == "aws-ec2"
        assert cfg.aws.region == "us-east-2"
        assert cfg.aws.profile == "p"
        assert cfg.security_group_ids == ["sg-1", "sg-2"]


class TestAppendBackendToYaml:
    def _write(self, path: Path, doc: dict) -> None:
        with open(path, "w") as f:
            yaml.safe_dump(doc, f, sort_keys=False)

    def _backend(self, name: str = "aws-ec2") -> dict:
        return build_backend_dict(
            name=name, region="us-east-2", profile=None,
            ami="ami-1", subnet_id="subnet-1", security_group_ids=[],
            instance_profile_arn="arn:x", default_image="img:t",
            default_instance_type="c5.xlarge", ssh_user="ec2-user",
            max_instances=20,
        )

    def test_appends_to_existing_yaml(self, tmp_path):
        cfg_path = tmp_path / "scripthut.yaml"
        self._write(cfg_path, {
            "backends": [{"name": "mercury-nb", "type": "slurm"}],
            "settings": {"poll_interval": 60},
        })
        backup = append_backend_to_yaml(cfg_path, self._backend())
        with open(cfg_path) as f:
            doc = yaml.safe_load(f)
        names = [b["name"] for b in doc["backends"]]
        assert names == ["mercury-nb", "aws-ec2"]
        assert doc["settings"] == {"poll_interval": 60}  # preserved
        assert backup.exists()

    def test_creates_yaml_when_missing(self, tmp_path):
        cfg_path = tmp_path / "scripthut.yaml"
        backup = append_backend_to_yaml(cfg_path, self._backend())
        assert cfg_path.exists()
        with open(cfg_path) as f:
            doc = yaml.safe_load(f)
        assert doc["backends"][0]["name"] == "aws-ec2"
        assert not backup.exists()  # nothing to back up

    def test_idempotent_replaces_same_name(self, tmp_path):
        cfg_path = tmp_path / "scripthut.yaml"
        first = self._backend()
        second = self._backend()
        second["max_instances"] = 99
        append_backend_to_yaml(cfg_path, first)
        append_backend_to_yaml(cfg_path, second)
        with open(cfg_path) as f:
            doc = yaml.safe_load(f)
        # Only one entry with that name, and it carries the second value.
        ec2 = [b for b in doc["backends"] if b["name"] == "aws-ec2"]
        assert len(ec2) == 1
        assert ec2[0]["max_instances"] == 99

    def test_does_not_clobber_other_backends(self, tmp_path):
        cfg_path = tmp_path / "scripthut.yaml"
        self._write(cfg_path, {
            "backends": [
                {"name": "mercury-nb", "type": "slurm"},
                {"name": "aws-batch", "type": "batch",
                 "aws": {"region": "us-east-1", "job_queue": "q"},
                 "default_image": "img:t"},
            ],
        })
        append_backend_to_yaml(cfg_path, self._backend("aws-ec2"))
        with open(cfg_path) as f:
            doc = yaml.safe_load(f)
        names = [b["name"] for b in doc["backends"]]
        assert names == ["mercury-nb", "aws-batch", "aws-ec2"]

    def test_full_config_validates_after_append(self, tmp_path):
        """End-to-end: appending a new backend yields a config the loader
        accepts (EC2BackendConfig schema is reachable through the discriminator)."""
        cfg_path = tmp_path / "scripthut.yaml"
        self._write(cfg_path, {"backends": []})
        append_backend_to_yaml(cfg_path, self._backend("aws-ec2"))
        with open(cfg_path) as f:
            raw = yaml.safe_load(f)
        cfg = ScriptHutConfig.model_validate(raw)
        assert len(cfg.ec2_backends) == 1
        assert cfg.ec2_backends[0].name == "aws-ec2"

    def test_refuses_non_mapping_yaml(self, tmp_path):
        cfg_path = tmp_path / "scripthut.yaml"
        cfg_path.write_text("- just a list\n")
        with pytest.raises(RuntimeError, match="mapping"):
            append_backend_to_yaml(cfg_path, self._backend())


class TestSubcommandDispatch:
    def test_help_invokable(self, capsys):
        """Subcommand argparse should be reachable via main(['--help'])."""
        from scripthut.setup.aws_ec2 import main
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "Guided setup" in out

"""Guided setup for the AWS EC2 backend.

Walks the user through:

1. Discovering / picking an AWS region, default VPC, and subnet.
2. Looking up the latest Amazon Linux 2023 AMI (or accepting a user-specified one).
3. Deploying the IAM CloudFormation stack
   (:data:`IAM_TEMPLATE_BODY` — kept in sync with
   ``cloudformation/scripthut-ec2-iam.yaml``).
4. Reading the stack outputs.
5. Appending a populated EC2 backend stanza to ``scripthut.yaml``
   (with a timestamped backup of the original).
6. Running a connectivity smoke test (``EC2Backend.is_available()``).

Invoked via ``scripthut setup-aws-ec2``.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripthut.config_schema import EC2BackendConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embedded CloudFormation template — kept in sync with
# cloudformation/scripthut-ec2-iam.yaml at the repo root by
# tests/test_setup_aws_ec2.py::test_embedded_template_matches_file.
# Update both when changing IAM permissions.
# ---------------------------------------------------------------------------

IAM_TEMPLATE_BODY = r"""AWSTemplateFormatVersion: '2010-09-09'
Description: >
  IAM resources for scripthut's AWS EC2-direct backend. Creates the
  task-instance role + instance profile attached to each task instance,
  and a managed policy you attach to whichever IAM principal scripthut
  runs as (your user, an SSO role, an EC2 instance role, etc).

  Outputs print the exact ARNs to drop into scripthut.yaml. Tear down
  with: aws cloudformation delete-stack --stack-name <name>

Parameters:
  ResourcePrefix:
    Type: String
    Default: scripthut
    Description: >
      Prefix for resource names. Keep short — AWS appends generated
      suffixes. Must start with a letter.
    MaxLength: 16
    MinLength: 1
    AllowedPattern: '^[a-zA-Z][a-zA-Z0-9-]*$'
  EnableECRReadAccess:
    Type: String
    Default: 'true'
    AllowedValues: ['true', 'false']
    Description: >
      If 'true', task instances can pull container images from ECR
      (attaches AmazonEC2ContainerRegistryReadOnly to the instance role).
      Set to 'false' if you only pull from public registries (Docker Hub,
      GHCR, etc) and want to minimize permissions.

Conditions:
  IncludeEcrReadOnly: !Equals [!Ref EnableECRReadAccess, 'true']

Resources:
  # ----- IAM role attached to every task EC2 instance ----------------------
  TaskInstanceRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub '${ResourcePrefix}-task-instance-role'
      Description: >
        Role attached to scripthut EC2 task instances. Provides SSM Agent
        registration (used by scripthut's SSM-tunnelled SSH probes) and
        optional ECR pull access. Add custom policies here if your
        workload needs S3 / DynamoDB / Secrets Manager access.
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: ec2.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
        - !If
            - IncludeEcrReadOnly
            - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
            - !Ref AWS::NoValue
      Tags:
        - Key: ManagedBy
          Value: scripthut-cloudformation
        - Key: Stack
          Value: !Ref AWS::StackName

  # Wraps the role so it can be attached to EC2 instances by RunInstances.
  TaskInstanceProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      InstanceProfileName: !Sub '${ResourcePrefix}-task-instance-profile'
      Roles:
        - !Ref TaskInstanceRole

  # ----- Managed policy for the principal that runs scripthut --------------
  # Doesn't get attached to anyone here — that's an explicit step the
  # operator takes for whichever user/role they use, so we don't make
  # surprising attachments.
  ScripthutControllerPolicy:
    Type: AWS::IAM::ManagedPolicy
    Properties:
      ManagedPolicyName: !Sub '${ResourcePrefix}-controller-policy'
      Description: >
        Permissions scripthut's EC2 backend needs. Attach to whichever
        IAM principal scripthut runs as. Includes EC2 lifecycle, SSM
        Session Manager, EC2 Instance Connect (ephemeral SSH key push),
        and iam:PassRole on the task instance role created by this stack.
      PolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Sid: EC2Lifecycle
            Effect: Allow
            Action:
              - ec2:RunInstances
              - ec2:DescribeInstances
              - ec2:TerminateInstances
              - ec2:CreateTags
              - ec2:DescribeSubnets
              - ec2:DescribeSecurityGroups
              - ec2:DescribeImages
            # ec2:DescribeSubnets / DescribeSecurityGroups don't support
            # resource-level scoping, so we use "*" here. Scope further via
            # tag conditions in production if your security policy requires.
            Resource: '*'
          - Sid: PassInstanceProfileRoleToEc2
            Effect: Allow
            Action: iam:PassRole
            Resource: !GetAtt TaskInstanceRole.Arn
            Condition:
              StringEquals:
                'iam:PassedToService': 'ec2.amazonaws.com'
          - Sid: SSMSessionForTunnelledSSH
            Effect: Allow
            Action:
              - ssm:StartSession
              - ssm:TerminateSession
              - ssm:DescribeSessions
              - ssm:GetConnectionStatus
            Resource: '*'
          - Sid: EC2InstanceConnectEphemeralKeys
            Effect: Allow
            Action: ec2-instance-connect:SendSSHPublicKey
            Resource: '*'

Outputs:
  InstanceProfileArn:
    Description: >-
      Drop into scripthut.yaml -> backends[*].instance_profile_arn for
      the EC2 backend.
    Value: !GetAtt TaskInstanceProfile.Arn
    Export:
      Name: !Sub '${AWS::StackName}-InstanceProfileArn'

  TaskInstanceRoleArn:
    Description: >-
      ARN of the IAM role wrapped by the instance profile (informational —
      it's the iam:PassRole resource inside ScripthutControllerPolicy).
    Value: !GetAtt TaskInstanceRole.Arn
    Export:
      Name: !Sub '${AWS::StackName}-TaskInstanceRoleArn'

  TaskInstanceRoleName:
    Description: >-
      Name of the task instance role. Use this if you want to attach
      additional inline / managed policies for what your container needs
      (e.g. S3 access).
    Value: !Ref TaskInstanceRole
    Export:
      Name: !Sub '${AWS::StackName}-TaskInstanceRoleName'

  ScripthutControllerPolicyArn:
    Description: >-
      Attach to the IAM user/role scripthut runs as. Examples:
        aws iam attach-user-policy --user-name <user> --policy-arn <this>
        aws iam attach-role-policy --role-name <role> --policy-arn <this>
    Value: !Ref ScripthutControllerPolicy
    Export:
      Name: !Sub '${AWS::StackName}-ScripthutControllerPolicyArn'
"""


# ---------------------------------------------------------------------------
# Tiny prompt helpers — stdlib only; we don't want a click/prompt_toolkit dep
# on the critical path of a setup script that's run maybe once per account.
# ---------------------------------------------------------------------------


def _prompt(question: str, default: str | None = None, *, allow_empty: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        try:
            answer = input(f"  > {question}{suffix}: ").strip()
        except EOFError:
            print()  # newline after Ctrl-D
            raise KeyboardInterrupt
        if answer:
            return answer
        if default is not None:
            return default
        if allow_empty:
            return ""
        print("    (required)")


def _confirm(question: str, default_yes: bool = True) -> bool:
    suffix = "[Y/n]" if default_yes else "[y/N]"
    while True:
        try:
            answer = input(f"  > {question} {suffix}: ").strip().lower()
        except EOFError:
            print()
            raise KeyboardInterrupt
        if not answer:
            return default_yes
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("    please answer y or n")


def _heading(title: str) -> None:
    print()
    print(f"━━ {title} ━━")


# ---------------------------------------------------------------------------
# AWS discovery helpers (boto3-only — no AWS CLI subprocess needed)
# ---------------------------------------------------------------------------


def _boto3_session(profile: str | None, region: str) -> Any:
    try:
        import boto3
    except ImportError as e:  # pragma: no cover
        raise SystemExit(
            "boto3 is not installed. Run: pip install 'scripthut[batch]'"
        ) from e
    kwargs: dict[str, Any] = {"region_name": region}
    if profile:
        kwargs["profile_name"] = profile
    return boto3.Session(**kwargs)


def discover_default_vpc(session: Any) -> tuple[str, str, str] | None:
    """Return ``(vpc_id, subnet_id, availability_zone)`` for the default VPC,
    or ``None`` if there isn't one in this region.
    """
    ec2 = session.client("ec2")
    vpcs = ec2.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}]
    ).get("Vpcs", [])
    if not vpcs:
        return None
    vpc_id = vpcs[0]["VpcId"]
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    ).get("Subnets", [])
    if not subnets:
        return None
    # Prefer a subnet that auto-assigns public IPs (simpler outbound routing).
    public = [s for s in subnets if s.get("MapPublicIpOnLaunch")]
    chosen = (public or subnets)[0]
    return vpc_id, chosen["SubnetId"], chosen["AvailabilityZone"]


def lookup_latest_al2023_ami(session: Any) -> str | None:
    """Return the latest AL2023 x86_64 AMI ID in the session's region (via SSM)."""
    ssm = session.client("ssm")
    try:
        resp = ssm.get_parameter(
            Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
        )
        return resp["Parameter"]["Value"]
    except Exception as e:
        logger.debug(f"AL2023 AMI lookup failed: {e}")
        return None


# ---------------------------------------------------------------------------
# CloudFormation deploy + outputs (boto3, with a simple create-or-update flow)
# ---------------------------------------------------------------------------


def deploy_iam_stack(
    session: Any,
    *,
    stack_name: str,
    template_body: str,
    parameters: dict[str, str],
) -> dict[str, str]:
    """Create-or-update the IAM stack and wait for it; return the outputs map."""
    cfn = session.client("cloudformation")
    cfn_params = [
        {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
    ]

    exists = False
    try:
        cfn.describe_stacks(StackName=stack_name)
        exists = True
    except Exception as e:  # ClientError "does not exist" is the happy path
        if "does not exist" not in str(e):
            raise

    op = "update" if exists else "create"
    print(f"    {op[0].upper() + op[1:]}-deploying CloudFormation stack '{stack_name}'…")

    try:
        if exists:
            cfn.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=cfn_params,
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
        else:
            cfn.create_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=cfn_params,
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
    except Exception as e:
        if "No updates are to be performed" in str(e):
            print("    Stack already up to date — reading existing outputs.")
            return _read_stack_outputs(cfn, stack_name)
        raise

    waiter_name = "stack_update_complete" if exists else "stack_create_complete"
    print("    Waiting for stack to reach a stable state (this can take a minute)…")
    cfn.get_waiter(waiter_name).wait(
        StackName=stack_name, WaiterConfig={"Delay": 5, "MaxAttempts": 60}
    )
    return _read_stack_outputs(cfn, stack_name)


def _read_stack_outputs(cfn: Any, stack_name: str) -> dict[str, str]:
    resp = cfn.describe_stacks(StackName=stack_name)
    outputs: dict[str, str] = {}
    for out in (resp["Stacks"][0].get("Outputs") or []):
        outputs[out["OutputKey"]] = out["OutputValue"]
    return outputs


# ---------------------------------------------------------------------------
# scripthut.yaml manipulation — load + append + dump (with backup)
# ---------------------------------------------------------------------------


def append_backend_to_yaml(config_path: Path, backend_dict: dict[str, Any]) -> Path:
    """Append ``backend_dict`` to ``backends`` in ``config_path``, backing up first.

    Returns the backup path. Comments in the original may be lost — we
    explicitly warn about this in the caller.
    """
    import yaml

    if config_path.exists():
        backup_path = config_path.with_suffix(
            config_path.suffix + f".bak.{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        )
        shutil.copy2(config_path, backup_path)
        with open(config_path) as f:
            doc = yaml.safe_load(f) or {}
    else:
        backup_path = config_path.with_suffix(config_path.suffix + ".missing")
        doc = {}

    if not isinstance(doc, dict):
        raise RuntimeError(
            f"{config_path} doesn't parse as a YAML mapping; refusing to overwrite."
        )

    backends = doc.get("backends")
    if not isinstance(backends, list):
        backends = []
    # Replace any existing backend with the same name (re-running the script
    # should be idempotent).
    name = backend_dict["name"]
    backends = [b for b in backends if not (isinstance(b, dict) and b.get("name") == name)]
    backends.append(backend_dict)
    doc["backends"] = backends

    with open(config_path, "w") as f:
        yaml.safe_dump(doc, f, sort_keys=False, default_flow_style=False)

    return backup_path


def build_backend_dict(
    *,
    name: str,
    region: str,
    profile: str | None,
    ami: str,
    subnet_id: str,
    security_group_ids: list[str],
    instance_profile_arn: str,
    default_image: str,
    default_instance_type: str,
    ssh_user: str,
    max_instances: int,
) -> dict[str, Any]:
    """Build the dict that goes into scripthut.yaml under ``backends:``."""
    aws: dict[str, Any] = {"region": region}
    if profile:
        aws["profile"] = profile
    return {
        "name": name,
        "type": "ec2",
        "aws": aws,
        "ami": ami,
        "subnet_id": subnet_id,
        "security_group_ids": list(security_group_ids),
        "instance_types": {"default": default_instance_type},
        "instance_profile_arn": instance_profile_arn,
        "default_image": default_image,
        "ssh_user": ssh_user,
        "max_instances": max_instances,
    }


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------


def smoke_test(backend_dict: dict[str, Any]) -> bool:
    """Instantiate EC2Backend and call ``is_available``. Returns success bool."""
    import asyncio

    from scripthut.backends.ec2 import EC2Backend
    from scripthut.config_schema import EC2BackendConfig

    cfg = EC2BackendConfig(**backend_dict)
    backend = EC2Backend(cfg, archive_root=Path("/tmp/scripthut-setup-smoketest"))
    try:
        return asyncio.run(backend.is_available())
    except Exception as e:
        print(f"    smoke test raised: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripthut setup-aws-ec2",
        description=(
            "Guided setup for scripthut's AWS EC2 backend. Discovers a VPC + "
            "subnet, deploys the IAM CloudFormation stack, appends a backend "
            "stanza to scripthut.yaml, and runs a connectivity smoke test."
        ),
    )
    parser.add_argument("--config", type=Path, default=Path("scripthut.yaml"),
                        help="Path to scripthut.yaml (default: ./scripthut.yaml)")
    parser.add_argument("--region", help="AWS region (skips the prompt)")
    parser.add_argument("--profile", help="AWS profile name (skips the prompt)")
    parser.add_argument("--backend-name", default=None,
                        help="Backend name in scripthut.yaml (default: aws-ec2)")
    parser.add_argument("--stack-name", default=None,
                        help="CloudFormation stack name (default: scripthut-ec2-iam)")
    parser.add_argument("--non-interactive", action="store_true",
                        help="Fail on missing values rather than prompting (for CI)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _argparser().parse_args(argv)

    print()
    print("ScriptHut — AWS EC2 backend setup")
    print("──────────────────────────────────")
    print()
    print("This will deploy a small CloudFormation stack with the IAM resources")
    print("scripthut's EC2 backend needs, then append a backend stanza to your")
    print(f"scripthut.yaml ({args.config}).")
    print()
    print("Press Ctrl-C any time to abort. Tear-down later with:")
    print("    aws cloudformation delete-stack --stack-name <stack>")

    try:
        return _main_interactive(args)
    except KeyboardInterrupt:
        print()
        print("Aborted by user.")
        return 130


def _main_interactive(args: argparse.Namespace) -> int:
    interactive = not args.non_interactive

    _heading("Credentials")
    profile = args.profile
    if profile is None and interactive:
        profile = _prompt("AWS profile (blank = default credential chain)",
                          default="", allow_empty=True) or None
    region = args.region
    if region is None and interactive:
        region = _prompt("AWS region", default="us-east-1")
    if not region:
        print("ERROR: --region is required in non-interactive mode.")
        return 2

    print(f"    Using profile={profile or '(default chain)'}, region={region}")
    session = _boto3_session(profile, region)

    _heading("Network discovery")
    vpc = discover_default_vpc(session)
    if vpc is None:
        print("    No default VPC found in this region.")
        if not interactive:
            return 2
        subnet_id = _prompt("Subnet ID to launch task instances in")
        sg_ids_str = _prompt("Security group IDs (comma-separated, blank for none)",
                             default="", allow_empty=True)
        sg_ids = [s.strip() for s in sg_ids_str.split(",") if s.strip()]
    else:
        vpc_id, subnet_id, az = vpc
        print(f"    Found default VPC {vpc_id} → subnet {subnet_id} in {az}")
        if interactive and not _confirm("Use this subnet?"):
            subnet_id = _prompt("Subnet ID")
        sg_ids: list[str] = []
        if interactive:
            sg_ids_str = _prompt(
                "Security group IDs (comma-separated, blank = use VPC default)",
                default="", allow_empty=True,
            )
            sg_ids = [s.strip() for s in sg_ids_str.split(",") if s.strip()]

    _heading("AMI")
    ami = None
    discovered = lookup_latest_al2023_ami(session)
    if discovered:
        print(f"    Latest Amazon Linux 2023 AMI: {discovered}")
        print("    NOTE: AL2023 has SSM Agent pre-installed but you must install")
        print("          docker yourself (e.g. dnf install -y docker; systemctl")
        print("          enable --now docker) — see docs/configuration.md.")
        if interactive and _confirm("Use this AMI?"):
            ami = discovered
    if ami is None:
        ami = _prompt("AMI ID (must have sshd + SSM Agent + docker)") if interactive else ""
    if not ami:
        print("ERROR: AMI is required.")
        return 2

    _heading("Backend configuration")
    backend_name = args.backend_name or (
        _prompt("Backend name (used in scripthut.yaml)", default="aws-ec2")
        if interactive else "aws-ec2"
    )
    instance_type = (
        _prompt("Default instance type for the 'default' partition", default="c5.xlarge")
        if interactive else "c5.xlarge"
    )
    default_image = (
        _prompt("Default container image (e.g. ghcr.io/your-org/workflow:latest)")
        if interactive else ""
    )
    if not default_image:
        print("ERROR: a default container image is required.")
        return 2
    ssh_user = (
        _prompt("OS user inside the AMI (ec2-user for AL2023, ubuntu for Ubuntu)",
                default="ec2-user")
        if interactive else "ec2-user"
    )
    max_instances = int(
        _prompt("max_instances (hard cap)", default="20") if interactive else "20"
    )

    _heading("CloudFormation IAM stack")
    stack_name = args.stack_name or (
        _prompt("Stack name", default="scripthut-ec2-iam")
        if interactive else "scripthut-ec2-iam"
    )
    enable_ecr = (
        _confirm("Allow task instances to pull from ECR (recommended)?", True)
        if interactive else True
    )
    outputs = deploy_iam_stack(
        session,
        stack_name=stack_name,
        template_body=IAM_TEMPLATE_BODY,
        parameters={
            "ResourcePrefix": "scripthut",
            "EnableECRReadAccess": "true" if enable_ecr else "false",
        },
    )
    print("    Stack outputs:")
    for k, v in outputs.items():
        print(f"      {k}: {v}")

    instance_profile_arn = outputs.get("InstanceProfileArn")
    controller_policy_arn = outputs.get("ScripthutControllerPolicyArn")
    if not instance_profile_arn or not controller_policy_arn:
        print("ERROR: stack outputs missing expected keys.")
        return 1

    _heading("Writing scripthut.yaml")
    backend_dict = build_backend_dict(
        name=backend_name,
        region=region,
        profile=profile,
        ami=ami,
        subnet_id=subnet_id,
        security_group_ids=sg_ids,
        instance_profile_arn=instance_profile_arn,
        default_image=default_image,
        default_instance_type=instance_type,
        ssh_user=ssh_user,
        max_instances=max_instances,
    )
    backup = append_backend_to_yaml(args.config, backend_dict)
    if backup.exists():
        print(f"    Backed up original to {backup}")
        print("    NOTE: PyYAML does not preserve comments — see the backup if you")
        print("          had hand-written comments you want to migrate.")
    print(f"    Appended backend '{backend_name}' to {args.config}")

    _heading("Smoke test")
    if smoke_test(backend_dict):
        print("    OK — backend is reachable.")
    else:
        print("    Could not reach the backend (boto3 call failed).")
        print("    Most common causes: profile / region mismatch, or the IAM policy")
        print("    has not yet been attached to your principal (next step).")

    _heading("Next steps")
    print("    1. Attach the controller policy to your IAM user/role:")
    print(f"       aws iam attach-user-policy \\")
    print(f"         --user-name <YOUR-USER> \\")
    print(f"         --policy-arn {controller_policy_arn}")
    print("       (use attach-role-policy + --role-name if you run under a role)")
    print()
    print("    2. Confirm your AMI has docker installed (or build one).")
    print()
    print(f"    3. Start scripthut:    scripthut --config {args.config}")
    print()
    print(f"    Tear down later:       aws cloudformation delete-stack --stack-name {stack_name}")
    print()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))

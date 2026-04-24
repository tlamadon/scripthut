# YAML Configuration

ScriptHut is configured via a `scripthut.yaml` file. By default, the application looks for this file in the current working directory. You can also specify a custom path when starting the server.

The configuration file has the following top-level sections:

```yaml
backends: [...]       # Remote compute backends (Slurm, PBS, AWS Batch, AWS EC2)
sources: [...]        # Git repository sources
workflows: [...]      # Task generators (SSH commands returning JSON)
projects: [...]       # Git projects with sflow.json files
environments: [...]   # Named environment variable sets
pricing: {...}        # EC2-equivalent cost estimation
settings: {...}       # Global application settings
```

All sections are optional and default to empty lists or sensible defaults.

---

## Backends

Backends define the remote compute systems where jobs are submitted. ScriptHut supports **Slurm**, **PBS/Torque**, **AWS Batch**, and **AWS EC2** backend types (plus ECS, which is planned). Each backend is identified by a unique `name` and discriminated by its `type` field.

Slurm and PBS backends connect to a cluster login node over SSH. AWS Batch is API-based (boto3). AWS EC2 is API-based for launch/terminate but uses SSH tunnelled through SSM Session Manager for per-instance probes and logs — see [AWS Batch Backend](#aws-batch-backend) and [AWS EC2 Backend](#aws-ec2-backend) for credential setup.

### Slurm Backend

```yaml
backends:
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: your_username
      key_path: ~/.ssh/id_rsa
      cert_path: ~/.ssh/id_rsa-cert.pub   # optional
      known_hosts: ~/.ssh/known_hosts      # optional
    account: pi-faculty       # optional
    login_shell: false        # optional, default: false
    max_concurrent: 100       # optional, default: 100
    clone_dir: ~/scripthut-repos  # optional, disk usage reported in UI
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. Referenced by workflows. |
| `type` | string | **required** | Must be `"slurm"`. |
| `ssh` | object | **required** | SSH connection settings (see [SSH Config](#ssh-config) below). |
| `account` | string | `null` | Slurm account to charge jobs to. Passed as `--account` to `sbatch`. |
| `login_shell` | boolean | `false` | If `true`, job scripts use `#!/bin/bash -l` to source your login profile (`.bash_profile`, etc.). |
| `max_concurrent` | integer | `100` | Maximum total concurrent jobs across all runs on this backend. Must be >= 1. |
| `clone_dir` | string | `~/scripthut-repos` | Path on the backend whose disk usage is shown in the backend status panel. Typically the parent directory where source repos are cloned. |

### PBS/Torque Backend

```yaml
backends:
  - name: pbs-cluster
    type: pbs
    ssh:
      host: pbs-login.cluster.edu
      user: your_username
      key_path: ~/.ssh/id_rsa
    account: my-project       # optional
    login_shell: false        # optional
    max_concurrent: 100       # optional
    queue: batch              # optional
    clone_dir: ~/scripthut-repos  # optional, disk usage reported in UI
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. |
| `type` | string | **required** | Must be `"pbs"`. |
| `ssh` | object | **required** | SSH connection settings. |
| `account` | string | `null` | PBS account (`-A` flag). |
| `login_shell` | boolean | `false` | Use login shell in job scripts. |
| `max_concurrent` | integer | `100` | Maximum concurrent jobs. |
| `queue` | string | `null` | Default PBS queue. Overrides the `partition` field in task definitions. |
| `clone_dir` | string | `~/scripthut-repos` | Path on the backend whose disk usage is shown in the backend status panel. Typically the parent directory where source repos are cloned. |

### AWS Batch Backend

AWS Batch is an API-based backend — it has no SSH host and runs tasks inside containers. Install the `[batch]` extra to pull in `boto3`:

```bash
pip install 'scripthut[batch]'
```

There are three ways to tell scripthut which job definition to use:

=== "Pre-registered, locked (simplest)"

    ```yaml
    backends:
      - name: aws-batch
        type: batch
        aws:
          profile: my-profile
          region: us-east-1
          job_queue: my-batch-queue
        job_definition: simpleJobDef-b760b37  # existing JD name, name:revision, or ARN
        job_definition_mode: locked            # default — always submit against this JD
        retry_attempts: 1
        log_group: /aws/batch/job
        max_concurrent: 50
    ```

    Scripthut skips `RegisterJobDefinition` entirely. Recommended when every task in your workflow uses the same container image, or when you can't call `batch:RegisterJobDefinition` at all.

    **The container image is locked** to whatever the definition was registered with. AWS Batch does not honor image overrides via `containerOverrides`. If a task's `image` differs from the registered image, scripthut logs a warning and the registered image wins. If you need per-task images, use the next mode.

=== "Pre-registered, revisions (per-image)"

    ```yaml
    backends:
      - name: aws-batch
        type: batch
        aws:
          region: us-east-1
          job_queue: my-batch-queue
        job_definition: simpleJobDef-b760b37
        job_definition_mode: revisions         # register new revisions as images change
        retry_attempts: 1
        max_concurrent: 50
    ```

    Scripthut treats the configured definition as a template. On first submit with a new image, it:

    1. Calls `DescribeJobDefinitions` to fetch the template (latest ACTIVE revision).
    2. Clones the template's `containerProperties` (roles, log config, resource requirements, etc.).
    3. Swaps in the task's requested image.
    4. Calls `RegisterJobDefinition` with the **same `jobDefinitionName`** — AWS auto-increments the revision.
    5. Caches `image → new revision ARN` in process; subsequent tasks with the same image skip the registration.

    Top-level fields carried over from the template: `type`, `parameters`, `schedulingPriority`, `retryStrategy`, `propagateTags`, `timeout`, `tags`, `platformCapabilities`, `nodeProperties`, `eksProperties`, `ecsProperties`.

    Good for workflows where different tasks use different images but share roles/networking/log config. Requires `batch:RegisterJobDefinition` and `batch:DescribeJobDefinitions`. `iam:PassRole` is still enforced by AWS on the template's role ARNs — if those are cross-account, this mode won't work.

=== "Auto-registered (no existing JD)"

    ```yaml
    backends:
      - name: aws-batch
        type: batch
        aws:
          region: us-east-1
          job_queue: my-batch-queue
        default_image: ghcr.io/org/workflow:latest
        job_role_arn: arn:aws:iam::123456789012:role/BatchJobRole       # optional
        execution_role_arn: arn:aws:iam::123456789012:role/BatchExec    # optional
        retry_attempts: 1
        max_concurrent: 50
    ```

    On first submission for a given image, scripthut registers a generic job definition named `scripthut-<hash>` and reuses it. The calling principal must be able to `iam:PassRole` on any role ARNs in the **same AWS account**.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. |
| `type` | string | **required** | Must be `"batch"`. |
| `aws` | object | **required** | AWS configuration (see below). |
| `job_definition` | string | `null` | Pre-registered Batch job definition (bare name, `name:revision`, or ARN). *Either `job_definition` or `default_image` must be set.* |
| `job_definition_mode` | string | `"locked"` | `"locked"` — submit against the configured JD verbatim; `"revisions"` — use it as a template and register new revisions per image. |
| `default_image` | string | `null` | Default container image URI used when scripthut auto-registers a definition. Ignored when `job_definition_mode == "locked"`. In `"revisions"` mode, used as a fallback when a task has no `image` set and the template's image doesn't apply. |
| `job_role_arn` | string | `null` | IAM role the container assumes at runtime (`jobRoleArn`). Used only for auto-registration (not revisions mode — roles come from the template). |
| `execution_role_arn` | string | `null` | IAM role ECS uses to pull the image and push logs (`executionRoleArn`). Same caveat as `job_role_arn`. |
| `retry_attempts` | integer | `1` | `retryStrategy.attempts` on submitted jobs. Allowed range 1–10. |
| `log_group` | string | `"/aws/batch/job"` | CloudWatch Logs group where Batch writes container logs. |
| `max_concurrent` | integer | `100` | Maximum total concurrent jobs across all runs on this backend. |

**AWS Config:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `profile` | string | `null` | AWS CLI profile name (from `~/.aws/credentials`). Omit to use the default credential chain. |
| `region` | string | **required** | AWS region (e.g. `us-east-1`). |
| `job_queue` | string | **required** | Default AWS Batch job queue name. Tasks whose `partition` is unset submit here. |

#### How task fields map to AWS Batch

| ScriptHut field | AWS Batch concept |
|-----------------|-------------------|
| `task.partition` | Batch job queue name (falls back to `aws.job_queue`) |
| `task.cpus` | `containerOverrides.resourceRequirements` VCPU |
| `task.memory` | `containerOverrides.resourceRequirements` MEMORY (MiB) |
| `task.time_limit` | `timeout.attemptDurationSeconds` (must be ≥ 60s; omitted otherwise) |
| `task.gres` (e.g. `"gpu:2"`) | GPU resource requirement |
| `task.image` or backend `default_image` | Container image URI |
| Generated bash script | `containerOverrides.command = ["bash", "-c", <script>]` |

On first submission for a given image, ScriptHut registers a generic job definition named `scripthut-<hash>` and reuses it for subsequent submissions. Per-task CPU/memory/GPU/command/timeout/environment/retry are all applied via `containerOverrides` (and `retryStrategy`) at submit time — a single job definition serves any number of tasks that share the same image.

**Environment variables** are delivered via `containerOverrides.environment`, not inlined as `export` lines in the bash script. This means the container entrypoint sees them at process start, not only after the script begins executing. A short comment block in the generated script lists the variable names for visibility in the UI; the values travel through the AWS API.

#### Cross-account pass role

`RegisterJobDefinition` requires `iam:PassRole` and enforces that role ARNs live in the **same AWS account** as the caller. If you see:

```
AccessDeniedException … RegisterJobDefinition … Cross-account pass role is not allowed
```

your `job_role_arn` / `execution_role_arn` are in a different account than your credentials. Fix either:

- Use ARNs from the account your credentials belong to; or
- Switch to same-account credentials; or
- **Simpler: set `job_definition:` instead.** Since `SubmitJob` does *not* re-check `iam:PassRole`, an existing job definition can reference roles from any account.

Run `aws sts get-caller-identity --query Account --output text` and compare against the account ID embedded in your role ARNs to confirm.

#### Logs

Batch writes stdout+stderr to a single CloudWatch Log stream per job. ScriptHut reads them via `logs:GetLogEvents`. The "error" log tab in the UI shows a note pointing you to the "output" tab.

#### Git workflows on Batch

There is no shared filesystem, so ScriptHut resolves the commit SHA locally (via `git ls-remote`) and passes it to each container as environment variables. The generated bash script includes a `git clone` + `git checkout` block that runs before the task command:

```bash
# Injected automatically for git workflows
_SCRIPTHUT_CLONE_DIR="${SCRIPTHUT_CLONE_DIR:-/tmp/scripthut-src}"
if [ ! -d "$_SCRIPTHUT_CLONE_DIR/.git" ]; then
    git clone "$SCRIPTHUT_GIT_REPO" "$_SCRIPTHUT_CLONE_DIR"
fi
(cd "$_SCRIPTHUT_CLONE_DIR" && git fetch --all --tags && git checkout $SCRIPTHUT_GIT_SHA)
```

The container must have `git` and `bash` installed. For private repositories, configure a deploy key inside the container image or mount credentials via the container's IAM role.

#### AWS Credentials

ScriptHut **never** stores AWS credentials in `scripthut.yaml`. It uses boto3's standard credential resolution chain:

1. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_DEFAULT_REGION`.
2. **Shared credentials file** — `~/.aws/credentials` selected via `aws.profile`. Populate with `aws configure` or `aws configure sso --profile <name>`.
3. **IAM role attached to the host** — when scripthut runs on EC2, ECS, EKS, or Fargate, the instance/task role is used automatically. This is the recommended setup for production deployments — no credentials are stored on disk.

Pick whichever matches how you operate scripthut:

=== "CLI profile (most common)"

    ```bash
    aws configure --profile scripthut
    # Enter access key, secret key, region
    ```

    ```yaml
    backends:
      - name: aws-batch
        type: batch
        aws:
          profile: scripthut
          region: us-east-1
          job_queue: my-queue
        default_image: ghcr.io/org/image:latest
    ```

=== "AWS SSO"

    ```bash
    aws configure sso --profile scripthut-sso
    aws sso login --profile scripthut-sso
    ```

    Then set `aws.profile: scripthut-sso`. The session token is refreshed automatically by boto3.

=== "Environment variables"

    ```bash
    export AWS_ACCESS_KEY_ID=AKIA...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_DEFAULT_REGION=us-east-1
    scripthut
    ```

    Leave `aws.profile` unset.

=== "IAM instance role"

    Attach an IAM role to the EC2 instance (or ECS task) running scripthut. No configuration needed on the scripthut side — boto3 picks it up from the instance metadata service automatically. Leave `aws.profile` unset.

#### Minimum IAM permissions

The principal ScriptHut runs as needs the following permissions. Scope the resource ARNs to your specific queue and log group in production:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BatchSubmitAndMonitor",
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:ListJobs",
        "batch:CancelJob",
        "batch:TerminateJob",
        "batch:RegisterJobDefinition",
        "batch:DescribeJobQueues",
        "batch:DescribeComputeEnvironments"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ReadBatchLogs",
      "Effect": "Allow",
      "Action": ["logs:GetLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/batch/job:*"
    }
  ]
}
```

If you set `job_role_arn` or `execution_role_arn`, the principal also needs `iam:PassRole` on those specific role ARNs.

### AWS EC2 Backend

Launches one dedicated EC2 instance per task, `docker run`s the task's container via user-data, reconciles completion over SSH **tunnelled through SSM Session Manager**, and terminates the instance after archiving the log. Authentication uses a one-shot SSH key pushed via EC2 Instance Connect (valid 60 s) per connection — there is no persistent key material, no EC2 KeyPair, and no inbound port 22.

This is the closest cloud analogue to the SSH-based Slurm/PBS backends: scripthut reads logs, probes state, and does post-mortem analysis over SSH. The differences: the "cluster" is ephemeral (one-off instances), the network path is a port-forwarded SSM session, and the log is archived to scripthut's local `<data_dir>/ec2-logs/<run_id>/<task_id>.log` on completion (since the instance is deleted).

```yaml
backends:
  - name: aws-ec2
    type: ec2
    aws:
      profile: scripthut                  # optional
      region: us-east-2
    ami: ami-0abc123...                   # must have sshd + SSM Agent + docker
    subnet_id: subnet-0abc...
    security_group_ids: [sg-0abc...]      # inbound port 22 NOT required
    instance_types:
      default: c5.xlarge
      gpu: g4dn.xlarge
    default_image: ghcr.io/org/image:latest
    instance_profile_arn: arn:aws:iam::123:instance-profile/ScriptHutTask
    ssh_user: ec2-user
    max_instances: 20
    idle_terminate_seconds: 1800          # safety slack past task.time_limit
    startup_timeout_seconds: 600
    tag_prefix: scripthut
    extra_tags:
      Environment: research
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. |
| `type` | string | **required** | Must be `"ec2"`. |
| `aws.profile` | string | `null` | AWS CLI profile (omit to use the default credential chain). |
| `aws.region` | string | **required** | AWS region. |
| `ami` | string | **required** | AMI used for every task instance. Must have sshd, SSM Agent, and docker. |
| `subnet_id` | string | **required** | VPC subnet where task instances launch. |
| `security_group_ids` | list of string | `[]` | Optional SG IDs. Inbound port 22 is **not required** — SSH is tunnelled via SSM. |
| `instance_types` | map[string, string] | `{"default": "c5.xlarge"}` | Maps `task.partition` to an EC2 instance type. Unknown partitions fall back to `"default"`. |
| `instance_profile_arn` | string | `null` | IAM instance profile attached to each task instance. Must include `AmazonSSMManagedInstanceCore`; add ECR / S3 permissions if your container needs them. |
| `default_image` | string | `null` | Default container image URI (required unless every task sets its own). |
| `ssh_user` | string | `"ec2-user"` | OS user scripthut logs in as. Use `"ubuntu"` for Ubuntu AMIs. |
| `max_instances` | integer | `20` | Hard cap on concurrent task instances. Submissions above this raise an error. |
| `max_concurrent` | integer | `100` | Per-backend concurrent task cap (same semantics as other backends). |
| `idle_terminate_seconds` | integer | `1800` | Safety timer slack past the task's `time_limit` — the instance self-terminates if nothing happens within `time_limit + idle_terminate_seconds`. |
| `startup_timeout_seconds` | integer | `600` | Scripthut fails a task if its instance hasn't reached `"running"` within this many seconds. |
| `tag_prefix` | string | `"scripthut"` | Tag namespace for scripthut-owned instances. Used by the startup reconciler. |
| `extra_tags` | map[string, string] | `{}` | Additional tags applied to every launched instance (useful for cost allocation). |

#### How it works

1. `submit_task` → `RunInstances` with `InstanceInitiatedShutdownBehavior=terminate` and a user-data script that:
   - Pulls the container image.
   - Runs it with the task's bash script mounted read-only and `env_vars` injected via `docker run -e`.
   - Tees output to `/var/log/scripthut/task.log`.
   - Writes the exit code to `/var/run/scripthut/done` (the sentinel file).
   - Arms a safety timer that `shutdown -h now`s the instance after `task.time_limit + idle_terminate_seconds`.
2. Each poll cycle, scripthut opens a one-shot SSM port-forward (`aws ssm start-session --document-name AWS-StartPortForwardingSession`), pushes an ephemeral SSH key via `ec2-instance-connect:SendSSHPublicKey`, connects `asyncssh` to `127.0.0.1:<forwarded port>`, and checks the sentinel.
3. If the sentinel exists → `scp` (via the same session) the log file to `<data_dir>/ec2-logs/<run_id>/<task_id>.log` and call `TerminateInstances`. The task moves to `COMPLETED` / `FAILED` based on the exit code.
4. On scripthut restart, a startup reconciler picks up `tag:<tag_prefix>:backend=<name>` instances and resumes polling them — so crashes don't orphan running work.

#### Environment variables

Env vars passed to `submit_task` are injected via `docker run -e KEY=VALUE`, so the container's entrypoint sees them at process start. The generated script lists their names in a comment for visibility.

#### Safety layers

Three layered safeguards keep cost bounded even if scripthut crashes:

1. **User-data safety timer** — `shutdown -h now` after `task.time_limit + idle_terminate_seconds`. Combined with `InstanceInitiatedShutdownBehavior=terminate`, the instance deletes itself regardless of scripthut state.
2. **Hard task timeout** — `timeout <task.time_limit>s docker run ...` kills runaway containers.
3. **Startup reconciler** — adopts orphans after a scripthut restart, resumes polling, archives logs, terminates on completion.

#### Requirements on the scripthut host

- AWS CLI (`aws`) on PATH
- [`session-manager-plugin`](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) on PATH (bridges SSM's WebSocket protocol to a local TCP port)
- `boto3` (from `pip install 'scripthut[batch]'` — same extra covers both Batch and EC2)
- Network reachability to AWS SSM endpoints (standard egress to `*.amazonaws.com`). **No inbound path to instances is required.**

#### AMI requirements

- `sshd` running and accepting the configured `ssh_user`
- SSM Agent installed and running (default on Amazon Linux 2/2023 and SSM-flavored Ubuntu AMIs)
- `docker` installed and running
- An IAM instance profile attached that includes `AmazonSSMManagedInstanceCore`

#### Minimum IAM for the principal scripthut runs as

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EC2Lifecycle",
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances", "ec2:DescribeInstances",
        "ec2:TerminateInstances", "ec2:CreateTags",
        "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassInstanceProfileRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "<instance_profile_role_arn>"
    },
    {
      "Sid": "SSMSessionAndEphemeralKey",
      "Effect": "Allow",
      "Action": [
        "ssm:StartSession", "ssm:TerminateSession", "ssm:DescribeSessions",
        "ec2-instance-connect:SendSSHPublicKey"
      ],
      "Resource": "*"
    }
  ]
}
```

No CloudWatch Logs permissions needed — logs travel over the SSH tunnel.

#### Quick IAM bootstrap (CloudFormation)

The IAM piece is the most error-prone part — two roles, one instance profile, an `iam:PassRole` cross-reference, and a managed policy that has to match scripthut's exact API surface. To skip the manual click-ops, deploy [`cloudformation/scripthut-ec2-iam.yaml`](https://github.com/tlamadon/scripthut/blob/main/cloudformation/scripthut-ec2-iam.yaml) — it creates the task instance role + instance profile + a managed policy you attach to whatever principal scripthut runs as.

```bash
# 1) Deploy the stack (one shot per AWS account/region).
aws cloudformation deploy \
  --template-file cloudformation/scripthut-ec2-iam.yaml \
  --stack-name scripthut-ec2-iam \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ResourcePrefix=scripthut EnableECRReadAccess=true

# 2) Read the outputs.
aws cloudformation describe-stacks \
  --stack-name scripthut-ec2-iam \
  --query 'Stacks[0].Outputs'
```

The outputs map directly into your config:

| Output key | Where it goes |
|---|---|
| `InstanceProfileArn` | `backends[*].instance_profile_arn` in `scripthut.yaml` |
| `ScripthutControllerPolicyArn` | Attach to your IAM user/role: `aws iam attach-user-policy --user-name <name> --policy-arn <this>` (or `attach-role-policy --role-name <role>` if scripthut runs under a role) |
| `TaskInstanceRoleArn` | Informational — referenced inside the controller policy's `iam:PassRole` |
| `TaskInstanceRoleName` | Use to attach extra policies the *container* needs (e.g. S3 access): `aws iam attach-role-policy --role-name <this> --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess` |

**Tear-down** is a single command — useful if you decide not to use the EC2 backend after all:

```bash
aws cloudformation delete-stack --stack-name scripthut-ec2-iam
```

What the stack does **not** create:

- VPC / subnet / security group — use the default VPC or pick existing ones.
- The AMI — bake your own (sshd + SSM Agent + docker) or use Amazon Linux 2023 + a small bootstrap.
- An IAM user — the controller policy is unattached on purpose; you decide which principal gets it (your own user, an SSO role, or an EC2 instance role if you run scripthut on AWS itself).

---

### ECS Backend

!!! warning "Not yet implemented"
    ECS backend support is planned but not yet available. For running containers in AWS, use the [AWS Batch backend](#aws-batch-backend) or [AWS EC2 backend](#aws-ec2-backend) above.

```yaml
backends:
  - name: production-ecs
    type: ecs
    aws:
      profile: my-aws-profile    # optional
      region: us-east-1
      cluster_name: my-cluster
    max_concurrent: 100
```

### SSH Config

SSH settings are shared by both Slurm and PBS backends.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | **required** | Hostname of the remote machine. |
| `port` | integer | `22` | SSH port. |
| `user` | string | **required** | SSH username. |
| `key_path` | path | `~/.ssh/id_rsa` | Path to SSH private key. Supports `~` expansion. |
| `cert_path` | path | `null` | Path to SSH certificate for certificate-based authentication. |
| `known_hosts` | path | `null` | Path to `known_hosts` file. If `null`, host key checking is disabled. |

---

## Sources

Sources are git repositories or backend filesystem paths containing workflow definitions. ScriptHut discovers workflow JSON files using the `workflows_glob` pattern (default: `.hut/workflows/*.json`). You can use glob wildcards like `**/*.hut.json` to match files recursively across any subdirectory. Each matched JSON file appears as a triggerable workflow on the Sources page.

For **git sources**, the repository is cloned locally for workflow discovery, and also cloned on the backend when a workflow is triggered (tasks run inside the cloned directory, just like git-based workflows).

For **path sources**, workflows are discovered via SSH on the backend, and tasks run with `working_dir` resolved relative to the source path.

### Git Source

```yaml
sources:
  - name: ml-jobs
    type: git
    url: git@github.com:your-org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key
    backend: hpc-cluster
    # workflows_glob: "**/*.hut.json"  # default: .hut/workflows/*.json
    # clone_dir: ~/scripthut-repos     # default
    # postclone: "rm -rf large_files"  # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"git"`. |
| `url` | string | **required** | Git repository URL. SSH format recommended. |
| `branch` | string | `"main"` | Branch to track. |
| `deploy_key` | path | `null` | Path to deploy key for this repository. |
| `backend` | string | **required** | Backend to submit discovered workflow tasks to. |
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files (supports `**` for recursive matching). |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning. |

### Path Source

```yaml
sources:
  - name: shared-workflows
    type: path
    path: /shared/project-workflows
    backend: hpc-cluster
    # workflows_glob: "**/*.hut.json"  # default: .hut/workflows/*.json
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"path"`. |
| `path` | string | **required** | Directory on the backend filesystem. |
| `backend` | string | **required** | Backend where this path exists and where tasks are submitted. |
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files (supports `**` for recursive matching). |

---

## Workflows

Workflows are the primary mechanism for submitting batch jobs. A workflow defines an SSH command that runs on a backend and returns a JSON list of tasks (see [Task JSON Format](task-json.md) for details on the expected output).

### Basic Workflow

```yaml
workflows:
  - name: ml-training
    backend: hpc-cluster
    command: "python /shared/scripts/get_training_tasks.py"
    max_concurrent: 5
    description: "ML model training pipeline"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this workflow. Shown in the UI. |
| `backend` | string | **required** | Name of a backend defined in the `backends` section. |
| `command` | string | **required** | Shell command executed via SSH that must print JSON to stdout. |
| `max_concurrent` | integer | `null` | Max concurrent tasks per run. If `null`, only the backend-level limit applies. |
| `description` | string | `""` | Human-readable description shown in the UI. |
| `git` | object | `null` | Optional git repository to clone on the backend before running the command. |

### Git Workflows

Git workflows clone a repository on the remote backend before executing the command. The command runs inside the cloned directory. This is useful when your task generator script lives in a repository.

```yaml
workflows:
  - name: ml-training-git
    backend: hpc-cluster
    git:
      repo: git@github.com:your-org/ml-pipelines.git
      branch: main
      deploy_key: ~/.ssh/ml-deploy-key
      clone_dir: ~/scripthut-repos
      postclone: "rm -rf large_files"
    command: "python get_tasks.py"
    max_concurrent: 5
    description: "ML training from git repo"
```

**Git Config Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `repo` | string | **required** | Git repository URL. SSH format recommended. |
| `branch` | string | `"main"` | Branch to clone. |
| `deploy_key` | path | `null` | Path to deploy key on the **local** machine. It is uploaded to the backend temporarily during the clone operation. |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning (e.g., to remove large files or install dependencies). |

When using a git workflow:

- The `command` runs with the clone directory as its working directory.
- Task `working_dir` values using `~` or relative paths are resolved relative to the clone directory.
- Git metadata is injected as environment variables into every task (see [Automatic Environment Variables](task-json.md#automatic-environment-variables)).

---

## Projects

Projects reference git repositories that already exist on the backend and contain `sflow.json` workflow definition files.

```yaml
projects:
  - name: my-project
    backend: hpc-cluster
    path: /home/user/my-project
    max_concurrent: 10
    description: "My research project"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this project. |
| `backend` | string | **required** | Name of the backend where the project lives. |
| `path` | string | **required** | Path to the git repository on the backend. |
| `max_concurrent` | integer | `null` | Default max concurrent tasks per run. |
| `description` | string | `""` | Human-readable description. |

---

## Environments

Named environments define sets of environment variables and initialization commands that can be referenced by tasks. This is useful for loading modules, setting up language-specific paths, or configuring runtime parameters.

```yaml
environments:
  - name: julia-1.10
    variables:
      JULIA_DEPOT_PATH: "/scratch/user/julia_depot"
      JULIA_NUM_THREADS: "8"
    extra_init: "module load julia/1.10"

  - name: python-ml
    variables:
      CUDA_VISIBLE_DEVICES: "0,1"
      PYTHONPATH: "/home/user/libs"
    extra_init: |
      module load cuda/12.0
      source /home/user/venvs/ml/bin/activate
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier. Referenced by the `environment` field in task definitions. |
| `variables` | object | `{}` | Key-value pairs exported as environment variables before the task command. |
| `extra_init` | string | `""` | Raw bash lines to run before the task command (e.g., `module load` commands). Runs after environment variable exports. |

Tasks reference environments by name in their JSON definition:

```json
{
  "id": "train-model",
  "name": "Train Model",
  "command": "julia train.jl",
  "environment": "julia-1.10"
}
```

See [Environment Variable Priority](task-json.md#environment-variable-priority) for how environment variables from different sources are merged.

---

## Pricing

Optional EC2-equivalent cost estimation. Maps scheduler partitions to EC2 instance types and fetches pricing data from `instances.vantage.sh`.

```yaml
pricing:
  region: us-east-1
  price_type: spot_avg
  partitions:
    standard: c5.xlarge
    gpu: p3.2xlarge
    highmem: r5.4xlarge
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `region` | string | `"us-east-1"` | AWS region for pricing lookup. |
| `price_type` | string | `"ondemand"` | Pricing type. One of: `ondemand`, `spot_avg`, `spot_min`, `spot_max`. |
| `partitions` | object | `{}` | Mapping of scheduler partition names to EC2 instance types. |

Cost estimates appear in the UI when tasks specify a `partition` that has a mapping defined here.

---

## Settings

Global application settings that control server behavior and data storage.

```yaml
settings:
  poll_interval: 60
  server_host: 127.0.0.1
  server_port: 8000
  data_dir: ~/.cache/scripthut
  sources_cache_dir: ~/.cache/scripthut/sources
  filter_user: your_username
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `poll_interval` | integer | `60` | Interval in seconds between job status polls. Minimum: 5. |
| `server_host` | string | `"127.0.0.1"` | Host to bind the web server to. Use `0.0.0.0` to listen on all interfaces. |
| `server_port` | integer | `8000` | Port to bind the web server to. |
| `data_dir` | path | `~/.cache/scripthut` | Base directory for all stored data (run history, logs). |
| `sources_cache_dir` | path | `<data_dir>/sources` | Directory to cache cloned repositories. |
| `filter_user` | string | `null` | Default username for the "My Jobs" filter in the UI. If `null`, all users' jobs are shown. |

---

## Configuration File Lookup

ScriptHut searches for configuration in the following order:

1. Explicit path passed via command-line argument
2. `./scripthut.yaml` in the current directory
3. `./scripthut.yml` in the current directory

All path fields (e.g., `key_path`, `data_dir`, `deploy_key`) support `~` expansion to the user's home directory.

---

## Complete Example

```yaml
backends:
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: researcher
      key_path: ~/.ssh/id_rsa
    account: pi-faculty
    login_shell: true
    max_concurrent: 50

  - name: pbs-cluster
    type: pbs
    ssh:
      host: pbs-login.cluster.edu
      user: researcher
      key_path: ~/.ssh/id_rsa
    queue: batch

sources:
  - name: ml-jobs
    type: git
    url: git@github.com:my-org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-deploy-key
    backend: hpc-cluster

workflows:
  - name: simple-tasks
    backend: hpc-cluster
    command: "python /shared/scripts/generate_tasks.py --count 10"
    max_concurrent: 5
    description: "Simple test tasks"

  - name: ml-training
    backend: hpc-cluster
    git:
      repo: git@github.com:my-org/ml-pipelines.git
      branch: main
      deploy_key: ~/.ssh/ml-deploy-key
      clone_dir: ~/scripthut-repos
    command: "python generate_tasks.py"
    max_concurrent: 3
    description: "ML training pipeline from git"

environments:
  - name: julia-env
    variables:
      JULIA_DEPOT_PATH: "/scratch/researcher/julia_depot"
      JULIA_NUM_THREADS: "8"
    extra_init: "module load julia/1.10"

pricing:
  region: us-east-1
  price_type: spot_avg
  partitions:
    normal: c5.xlarge
    gpu: p3.2xlarge

settings:
  poll_interval: 30
  server_host: 127.0.0.1
  server_port: 8000
  data_dir: ~/.cache/scripthut
  filter_user: researcher
```

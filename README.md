# ScriptRun

A Python web interface to start and track jobs on remote systems like Slurm, ECS, and AWS Batch over SSH.

## Features

- **Multi-cluster support** - Monitor multiple Slurm/ECS clusters from a single dashboard
- **Real-time job monitoring** - View running and pending jobs with auto-refresh
- **Git source integration** - Clone job repositories with deploy key support
- **Persistent SSH connections** - Maintains connections with keepalive and auto-reconnect
- **HTMX frontend** - Dynamic updates without full page reloads
- **Type-safe** - Full type annotations with mypy strict mode support
- **Extensible** - Abstract backend system ready for ECS/AWS Batch support

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/scriptrun.git
cd scriptrun

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package
pip install -e .

# For development (includes mypy, ruff, pytest)
pip install -e ".[dev]"
```

## Configuration

ScriptRun uses a YAML configuration file. Copy the example and customize:

```bash
cp scriptrun.example.yaml scriptrun.yaml
```

### YAML Configuration

```yaml
# scriptrun.yaml

clusters:
  # Slurm cluster
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: researcher
      key_path: ~/.ssh/id_rsa

  # ECS cluster (coming soon)
  - name: production-ecs
    type: ecs
    aws:
      profile: my-aws-profile
      region: us-east-1
      cluster_name: my-ecs-cluster

# Git repositories with job definitions
sources:
  - name: ml-jobs
    url: git@github.com:org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key

settings:
  poll_interval: 60
  server_host: 127.0.0.1
  server_port: 8000
  sources_cache_dir: ~/.cache/scriptrun/sources
```

### Configuration Options

#### Clusters

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the cluster |
| `type` | Cluster type: `slurm` or `ecs` |
| `ssh.host` | SSH hostname (Slurm only) |
| `ssh.port` | SSH port (default: 22) |
| `ssh.user` | SSH username |
| `ssh.key_path` | Path to SSH private key |
| `aws.profile` | AWS CLI profile name (ECS only) |
| `aws.region` | AWS region (ECS only) |
| `aws.cluster_name` | ECS cluster name |

#### Sources

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the source |
| `url` | Git repository URL (SSH format recommended) |
| `branch` | Branch to track (default: main) |
| `deploy_key` | Path to deploy key for authentication |

#### Settings

| Field | Description | Default |
|-------|-------------|---------|
| `poll_interval` | Seconds between job polls | `60` |
| `server_host` | Web server bind host | `127.0.0.1` |
| `server_port` | Web server bind port | `8000` |
| `sources_cache_dir` | Directory for cloned repos | `~/.cache/scriptrun/sources` |

### Legacy .env Support

For backwards compatibility, ScriptRun also supports `.env` files for single-cluster setups:

```env
SSH_HOST=slurm-login.cluster.edu
SSH_USER=researcher
SSH_KEY_PATH=~/.ssh/cluster_key
POLL_INTERVAL=30
```

> **Note**: The `.env` format is deprecated. Please migrate to `scriptrun.yaml`.

## Usage

```bash
# Use default config (./scriptrun.yaml)
scriptrun

# Specify config file
scriptrun --config /path/to/config.yaml

# Override host/port
scriptrun --host 0.0.0.0 --port 9000
```

Open http://127.0.0.1:8000 in your browser.

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Main page with job list |
| `GET /jobs` | HTMX partial for job table |
| `GET /jobs/stream` | SSE endpoint for live updates |
| `GET /health` | Health check (JSON) |
| `GET /sources` | List configured sources |
| `POST /sources/{name}/sync` | Trigger source sync |

## Architecture

```
src/scriptrun/
├── main.py           # FastAPI app, routes, background polling
├── config.py         # Configuration loading (YAML + .env)
├── config_schema.py  # Pydantic models for YAML schema
├── models.py         # Data models (SlurmJob, JobState, ConnectionStatus)
├── ssh/
│   └── client.py     # Async SSH client with connection management
├── backends/
│   ├── base.py       # Abstract JobBackend interface
│   └── slurm.py      # Slurm implementation (squeue parsing)
└── sources/
    └── git.py        # Git repository management with deploy keys
```

### Adding New Backends

To add support for a new job system (e.g., AWS Batch):

1. Create a new file in `src/scriptrun/backends/` (e.g., `batch.py`)
2. Implement the `JobBackend` abstract class
3. Define appropriate job models in `models.py`

```python
from scriptrun.backends.base import JobBackend

class BatchBackend(JobBackend):
    @property
    def name(self) -> str:
        return "aws-batch"

    async def get_jobs(self, user: str | None = None) -> list[BatchJob]:
        # Implementation here
        ...

    async def is_available(self) -> bool:
        # Implementation here
        ...
```

## Development

```bash
# Run type checking
mypy src/

# Run linter
ruff check src/

# Run tests
pytest
```

## Roadmap

- [x] **Phase 1**: Multi-cluster Slurm monitoring
- [x] **Phase 1**: Git source integration with deploy keys
- [ ] **Phase 2**: Submit jobs to Slurm from UI
- [ ] **Phase 3**: ECS/AWS Batch support
- [ ] **Phase 4**: Job logs and monitoring

## Requirements

- Python 3.11+
- SSH access to remote Slurm clusters with key-based authentication

## License

MIT

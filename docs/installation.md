# Installation & Running

ScriptHut can be installed via pip or run with Docker. It requires **Python 3.11+**.

---

## Prerequisites

Before installing ScriptHut, ensure you have:

- **SSH key-based authentication** set up for your remote clusters
- Your SSH private key accessible at the path you'll configure (default: `~/.ssh/id_rsa`)
- Network access to your HPC login nodes from the machine running ScriptHut

---

## Install with pip

### From PyPI

```bash
pip install scripthut
```

### From source

```bash
git clone https://github.com/tlamadon/scripthut.git
cd scripthut
pip install -e .
```

### Development install

If you want to contribute or run tests:

```bash
pip install -e ".[dev,docs]"
```

This installs additional dependencies for linting (`ruff`, `mypy`), testing (`pytest`), and documentation (`mkdocs-material`, `mike`).

---

## Running ScriptHut

### Quick start

1. Create a `scripthut.yaml` in your working directory (see [Configuration](configuration.md) for the full reference):

    ```yaml
    backends:
      - name: my-cluster
        type: slurm
        ssh:
          host: login.cluster.edu
          user: your_username
          key_path: ~/.ssh/id_rsa

    workflows:
      - name: test
        backend: my-cluster
        command: "echo '{\"tasks\": [{\"id\": \"hello\", \"name\": \"Hello\", \"command\": \"echo hello\"}]}'"
        description: "Test workflow"
    ```

2. Start the server:

    ```bash
    scripthut
    ```

3. Open [http://localhost:8000](http://localhost:8000) in your browser.

### Command-line options

```
scripthut [--config PATH] [--host HOST] [--port PORT]
```

| Option | Short | Description |
|--------|-------|-------------|
| `--config` | `-c` | Path to configuration file. Default: `./scripthut.yaml` or `./scripthut.yml`. |
| `--host` | | Host to bind the server to. Overrides the `settings.server_host` value in the config. |
| `--port` | `-p` | Port to bind the server to. Overrides the `settings.server_port` value in the config. |

Examples:

```bash
# Use a specific config file
scripthut --config /path/to/my-config.yaml

# Bind to all interfaces on port 9000
scripthut --host 0.0.0.0 --port 9000
```

---

## Running with Docker

ScriptHut publishes a Docker image at `ghcr.io/tlamadon/scripthut`.

### Docker run

```bash
docker run -d \
  --name scripthut \
  -p 8000:8000 \
  -v $(pwd)/scripthut.yaml:/app/scripthut.yaml:ro \
  -v ~/.ssh:/root/.ssh:ro \
  ghcr.io/tlamadon/scripthut:latest
```

This:

- Mounts your `scripthut.yaml` configuration into the container
- Mounts your SSH keys so the container can connect to remote clusters
- Exposes the web UI on port 8000

### Docker Compose

Create a `docker-compose.yml` file:

```yaml
services:
  scripthut:
    image: ghcr.io/tlamadon/scripthut:latest
    container_name: scripthut
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      # Configuration file
      - ./scripthut.yaml:/app/scripthut.yaml:ro
      # SSH keys for connecting to remote clusters
      - ~/.ssh:/root/.ssh:ro
      # Persistent data (run history, logs, cached repos)
      - scripthut-data:/root/.cache/scripthut

volumes:
  scripthut-data:
```

Then start it:

```bash
docker compose up -d
```

To view logs:

```bash
docker compose logs -f scripthut
```

To stop:

```bash
docker compose down
```

### Building the image locally

If you want to build from source:

```bash
git clone https://github.com/tlamadon/scripthut.git
cd scripthut
docker build -t scripthut .
```

Then use `scripthut` instead of `ghcr.io/tlamadon/scripthut:latest` in the commands above.

### Docker Compose with local build

```yaml
services:
  scripthut:
    build: .
    container_name: scripthut
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./scripthut.yaml:/app/scripthut.yaml:ro
      - ~/.ssh:/root/.ssh:ro
      - scripthut-data:/root/.cache/scripthut

volumes:
  scripthut-data:
```

---

## SSH Key Considerations

### File permissions

SSH keys must have correct permissions, both on the host and inside containers:

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
```

### Certificate-based auth

If your cluster uses SSH certificates, configure the `cert_path` in your backend's SSH config:

```yaml
backends:
  - name: my-cluster
    type: slurm
    ssh:
      host: login.cluster.edu
      user: your_username
      key_path: ~/.ssh/id_rsa
      cert_path: ~/.ssh/id_rsa-cert.pub
```

### Known hosts

By default, ScriptHut does not verify host keys. To enable host key verification:

```yaml
ssh:
  host: login.cluster.edu
  user: your_username
  key_path: ~/.ssh/id_rsa
  known_hosts: ~/.ssh/known_hosts
```

---

## Verifying the installation

After starting ScriptHut, you should see output similar to:

```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

Navigate to the URL in your browser. The dashboard should show your configured backends and workflows. If a backend connection fails, check the terminal output for SSH error messages and verify your SSH configuration.

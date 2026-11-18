"""SSH client construction from backend configuration."""

from scripthut.config_schema import SlurmBackendConfig, PBSBackendConfig
from scripthut.ssh.client import SSHClient


def ssh_client_for(backend_cfg: SlurmBackendConfig | PBSBackendConfig) -> SSHClient:
    """Build an SSHClient from backend connection settings."""
    return SSHClient(
        host=backend_cfg.ssh.host,
        user=backend_cfg.ssh.user,
        key_path=backend_cfg.ssh.key_path_resolved,
        port=backend_cfg.ssh.port,
        cert_path=backend_cfg.ssh.cert_path_resolved,
        known_hosts=backend_cfg.ssh.known_hosts_resolved,
    )

"""Source metadata shared by the API and CLI."""

from typing import Any


def source_summary(src: Any) -> dict[str, Any]:
    """Compact dict for a GitSourceConfig or PathSourceConfig.

    Hides type-specific fields under a discriminator so callers can
    format both variants without sniffing the class.
    """
    from scripthut.config_schema import GitSourceConfig, PathSourceConfig
    common: dict[str, Any] = {
        "name": src.name,
        "type": src.type,
        "description": getattr(src, "description", ""),
    }
    if isinstance(src, GitSourceConfig):
        common.update({"url": src.url, "branch": src.branch})
        if src.local_path is not None:
            common["local_path"] = str(src.local_path_resolved)
    elif isinstance(src, PathSourceConfig):
        common.update({"path": src.path, "backend": src.backend})
    return common

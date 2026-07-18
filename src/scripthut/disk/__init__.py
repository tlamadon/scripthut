"""Remote disk-usage scanning: inventory what ScriptHut leaves on backends."""

from scripthut.disk.classify import (
    RunReferences,
    build_run_references,
    classify_entries,
    normalize_remote_path,
)
from scripthut.disk.cleanup import (
    CleanupOutcome,
    CleanupPlan,
    CleanupPlanEntry,
    CleanupReport,
    plan_cleanup,
)
from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
    ScanSpec,
)
from scripthut.disk.scan import (
    build_scan_script,
    build_scan_spec,
    parse_scan_output,
    raw_to_entries,
)
from scripthut.disk.service import DiskScanService

__all__ = [
    "CleanupOutcome",
    "CleanupPlan",
    "CleanupPlanEntry",
    "CleanupReport",
    "DiskEntry",
    "DiskEntryClass",
    "DiskEntryKind",
    "DiskScanResult",
    "DiskScanService",
    "RunReferences",
    "ScanSpec",
    "build_run_references",
    "build_scan_script",
    "build_scan_spec",
    "classify_entries",
    "normalize_remote_path",
    "parse_scan_output",
    "plan_cleanup",
    "raw_to_entries",
]

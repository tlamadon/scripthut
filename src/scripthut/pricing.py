"""EC2-equivalent cost estimation using instances.vantage.sh pricing data."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripthut.config_schema import PricingConfig
    from scripthut.runs.models import Run, RunItem

logger = logging.getLogger(__name__)

INSTANCES_URL = "https://instances.vantage.sh/instances.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours


@dataclass
class InstanceInfo:
    """Cached pricing info for an EC2 instance type."""

    vcpus: int
    price_per_hour: float


@dataclass
class RunCostSummary:
    """Cost summary for a run."""

    total_cost: float
    costed_tasks: int
    uncosted_tasks: int


class PricingService:
    """Fetches and caches EC2 pricing data, computes per-run costs."""

    def __init__(self, config: PricingConfig, cache_dir: Path) -> None:
        self._config = config
        self._cache_path = cache_dir / "pricing_cache.json"
        self._lookup: dict[str, InstanceInfo] = {}

    @property
    def ready(self) -> bool:
        return bool(self._lookup)

    async def initialize(self) -> None:
        """Load pricing data from cache or fetch from network."""
        instances = self._load_cache()
        if instances is None:
            instances = await self._fetch()
            if instances is not None:
                self._save_cache(instances)

        if instances is not None:
            self._build_lookup(instances)
            logger.info(
                f"Pricing service ready: {len(self._lookup)} instance types "
                f"for region={self._config.region}, price_type={self._config.price_type}"
            )

    def _load_cache(self) -> list | None:
        try:
            if not self._cache_path.exists():
                return None
            age = time.time() - self._cache_path.stat().st_mtime
            if age > CACHE_TTL_SECONDS:
                logger.info("Pricing cache expired, will re-fetch")
                return None
            with open(self._cache_path) as f:
                data = json.load(f)
            logger.info(f"Loaded pricing data from cache ({len(data)} instances)")
            return data
        except Exception as e:
            logger.warning(f"Failed to load pricing cache: {e}")
            return None

    def _save_cache(self, data: list) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump(data, f)
            logger.info(f"Saved pricing cache to {self._cache_path}")
        except Exception as e:
            logger.warning(f"Failed to save pricing cache: {e}")

    async def _fetch(self) -> list | None:
        """Fetch instances.json from vantage.sh (in a thread to avoid blocking)."""
        def _do_fetch() -> list:
            logger.info(f"Fetching pricing data from {INSTANCES_URL}")
            req = urllib.request.Request(INSTANCES_URL, headers={"User-Agent": "scripthut"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())

        try:
            return await asyncio.to_thread(_do_fetch)
        except Exception as e:
            logger.warning(f"Failed to fetch pricing data: {e}")
            return None

    def _build_lookup(self, instances: list) -> None:
        """Build {instance_type: InstanceInfo} lookup from raw JSON."""
        region = self._config.region
        price_type = self._config.price_type

        for inst in instances:
            itype = inst.get("instance_type")
            vcpus = inst.get("vCPU")
            if not itype or not vcpus:
                continue

            region_pricing = inst.get("pricing", {}).get(region, {})
            linux_pricing = region_pricing.get("linux", {})

            price_str = linux_pricing.get(price_type)
            if not price_str:
                continue

            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue

            self._lookup[itype] = InstanceInfo(vcpus=int(vcpus), price_per_hour=price)

    def compute_task_cost(self, item: RunItem) -> float | None:
        """Compute cost for a single task. Returns None if not computable."""
        if item.started_at is None or item.finished_at is None:
            return None

        if not item.task.partition:
            return None

        instance_type = self._config.partitions.get(item.task.partition)
        if not instance_type:
            return None

        info = self._lookup.get(instance_type)
        if not info:
            return None

        elapsed_seconds = (item.finished_at - item.started_at).total_seconds()
        if elapsed_seconds <= 0:
            return 0.0

        elapsed_hours = elapsed_seconds / 3600.0
        fraction = item.task.cpus / max(info.vcpus, 1)
        return elapsed_hours * fraction * info.price_per_hour

    def compute_run_cost(self, run: Run) -> RunCostSummary:
        """Compute cost summary for an entire run."""
        total = 0.0
        costed = 0
        uncosted = 0

        for item in run.items:
            cost = self.compute_task_cost(item)
            if cost is not None:
                total += cost
                costed += 1
            else:
                uncosted += 1

        return RunCostSummary(
            total_cost=round(total, 4),
            costed_tasks=costed,
            uncosted_tasks=uncosted,
        )

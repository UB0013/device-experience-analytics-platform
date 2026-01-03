"""HDFS storage manager — handles data lake organization and lifecycle."""

import subprocess
from datetime import datetime, timedelta
from typing import Optional

from src.utils.config import get_hdfs_config
from src.utils.logging import setup_logging

logger = setup_logging("hdfs-manager")


class HDFSManager:
    """Manages HDFS data lake structure and operations."""

    def __init__(self):
        cfg = get_hdfs_config()
        self.namenode = cfg["namenode"]
        self.paths = cfg["paths"]

    def _hdfs_cmd(self, *args) -> tuple[int, str]:
        cmd = ["hdfs", "dfs", "-fs", self.namenode] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode, result.stdout + result.stderr

    def init_data_lake(self):
        """Create the full data lake directory structure."""
        dirs = [
            f"{self.paths['raw']}/app_events",
            f"{self.paths['raw']}/crashes",
            f"{self.paths['raw']}/performance",
            f"{self.paths['processed']}/aggregates",
            f"{self.paths['processed']}/metrics/active_users",
            f"{self.paths['processed']}/metrics/error_spikes",
            f"{self.paths['processed']}/metrics/feature_usage",
            f"{self.paths['processed']}/batch/daily_trends",
            f"{self.paths['processed']}/batch/regional_trends",
            f"{self.paths['processed']}/batch/feature_adoption",
            f"{self.paths['processed']}/batch/feature_ranking",
            f"{self.paths['processed']}/batch/retention_cohorts",
            f"{self.paths['processed']}/batch/crash_analysis",
            f"{self.paths['processed']}/batch/device_performance",
            f"{self.paths['historical']}",
            f"{self.paths['checkpoints']}",
        ]
        for d in dirs:
            rc, out = self._hdfs_cmd("-mkdir", "-p", d)
            if rc == 0:
                logger.info("directory_created", path=d)
            else:
                logger.error("directory_creation_failed", path=d, output=out)

        # Set permissions
        self._hdfs_cmd("-chmod", "-R", "777", "/data")
        self._hdfs_cmd("-chmod", "-R", "777", "/spark")
        logger.info("data_lake_initialized")

    def list_directory(self, path: str) -> list[str]:
        rc, out = self._hdfs_cmd("-ls", path)
        if rc != 0:
            return []
        return [line.split()[-1] for line in out.strip().split("\n")[1:] if line.strip()]

    def get_storage_stats(self) -> dict:
        """Get storage usage statistics."""
        rc, out = self._hdfs_cmd("-du", "-s", "-h", "/data")
        return {"raw_output": out.strip(), "status": "ok" if rc == 0 else "error"}

    def archive_old_data(self, days_old: int = 30):
        """Move data older than N days from processed to historical."""
        cutoff = (datetime.utcnow() - timedelta(days=days_old)).strftime("%Y-%m-%d")
        logger.info("archiving_data", older_than=cutoff)
        # Implementation would scan partitions and move old ones
        # This is a placeholder for the archival logic
        logger.info("archive_complete")

    def cleanup_checkpoints(self, keep_latest: int = 5):
        """Remove old checkpoint directories, keeping the N most recent."""
        checkpoints = self.list_directory(self.paths["checkpoints"])
        if len(checkpoints) > keep_latest:
            to_remove = sorted(checkpoints)[:-keep_latest]
            for cp in to_remove:
                self._hdfs_cmd("-rm", "-r", cp)
                logger.info("checkpoint_removed", path=cp)

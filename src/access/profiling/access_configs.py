# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.config_profiling import ProfilingLog
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.payu_config import PayuConfigProfiling
from access.profiling.um_parser import UMProfilingParser

logger = logging.getLogger(__name__)


class ESM16ConfigProfiling(PayuConfigProfiling):
    """Handles profiling of ACCESS-ESM1.6 configurations."""

    def get_component_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in ACCESS-ESM1.6.

        Args:
            path (Path): Path to the output directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.
        """
        logs = {}
        um_logfile = path / "atmosphere" / "atm.fort6.pe0"
        if um_logfile.is_file():
            logger.debug(f"Found UM log file: {um_logfile}")
            logs["UM"] = ProfilingLog(um_logfile, UMProfilingParser())

        mom5_logfile = path / "access-esm1.6.out"
        if mom5_logfile.is_file():
            logger.debug(f"Found MOM5 log file: {mom5_logfile}")
            logs["MOM5"] = ProfilingLog(mom5_logfile, FMSProfilingParser(has_hits=False))

        cice5_logfile = path / "ice" / "ice_diag.d"
        if cice5_logfile.is_file():
            logger.debug(f"Found CICE5 log file: {cice5_logfile}")
            logs["CICE5"] = ProfilingLog(cice5_logfile, CICE5ProfilingParser())

        return logs

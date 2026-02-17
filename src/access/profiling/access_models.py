# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

from access.config import YAMLParser
from access.config.esm1p6_layout_input import (
    LayoutSearchConfig,
    LayoutTuple,
    generate_esm1p6_core_layouts_from_node_count,
    generate_esm1p6_perturb_block,
)

from access.profiling.cice5_parser import CICE5ProfilingParser
from access.profiling.cylc_manager import CylcRoseManager
from access.profiling.experiment import ProfilingLog
from access.profiling.fms_parser import FMSProfilingParser
from access.profiling.payu_manager import PayuManager
from access.profiling.um_parser import UMProfilingParser, UMTotalRuntimeParser

logger = logging.getLogger(__name__)


class ESM16Profiling(PayuManager):
    """Handles profiling of ACCESS-ESM1.6 configurations."""

    @property
    def model_type(self) -> str:
        return "access-esm1.6"

    def get_component_logs(self, path: Path) -> dict[str, ProfilingLog]:
        """Returns available profiling logs for the components in ACCESS-ESM1.6.

        Args:
            path (Path): Path to the output directory.
        Returns:
            dict[str, ProfilingLog]: Dictionary mapping component names to their ProfilingLog instances.
        """
        logs = {}
        parser = YAMLParser()

        um_env_path = path / "atmosphere" / "um_env.yaml"
        um_env = parser.parse(um_env_path.read_text())
        um_logfile = path / "atmosphere" / f"{um_env['UM_STDOUT_FILE']}0"
        if um_logfile.is_file():
            logger.debug(f"Found UM log file: {um_logfile}")
            logs["UM"] = ProfilingLog(um_logfile, UMProfilingParser())
            logs["UM_Total_Walltime"] = ProfilingLog(um_logfile, UMTotalRuntimeParser())

        config_path = path / "config.yaml"
        payu_config = parser.parse(config_path.read_text())
        mom5_logfile = path / f"{payu_config['model']}.out"
        if mom5_logfile.is_file():
            logger.debug(f"Found MOM5 log file: {mom5_logfile}")
            logs["MOM5"] = ProfilingLog(mom5_logfile, FMSProfilingParser(has_hits=False))

        cice5_logfile = path / "ice" / "ice_diag.d"
        if cice5_logfile.is_file():
            logger.debug(f"Found CICE5 log file: {cice5_logfile}")
            logs["CICE5"] = ProfilingLog(cice5_logfile, CICE5ProfilingParser())

        return logs

    def generate_core_layouts_from_node_count(
        self, num_nodes: float, cores_per_node: int, layout_search_config: LayoutSearchConfig | None = None
    ) -> list:
        return generate_esm1p6_core_layouts_from_node_count(
            num_nodes, cores_per_node, layout_search_config=layout_search_config
        )

    def generate_perturbation_block(self, layout: LayoutTuple, branch_name_prefix: str) -> dict:
        return generate_esm1p6_perturb_block(layout, branch_name_prefix)


class RAM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-rAM3 configurations."""

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }

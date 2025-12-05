# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

from access.config import YAMLParser
from access.config.esm1p6_layout_input import generate_esm1p6_core_layouts_from_node_count, generate_esm1p6_perturb_block, LayoutSearchConfig
from experiment_generator.experiment_generator import ExperimentGenerator

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

    def generate_scaling_experiments(self, num_nodes_list: list[float], control_options: dict) -> None:
        """Generates scaling experiments for ACCESS-ESM1.6.

        Args:
            num_nodes_list (list[int]): List of number of nodes to generate experiments for.
        """

        generator_config = {
            "model_type": self.model_type,
            "repository_url": self._repository,
            "start_point": self._control_commit,
            "test_path": str(self.work_dir),
            "repository_directory": self._repository_directory,
            "control_branch_name": "ctrl",
            "Control_Experiment": control_options,
        }

        queue = "normalsr"
        branch_name_prefix = "esm1p6-layout"

        tol_around_ctrl_ratio = 0.05

        seen_layouts = set()
        walltime_hrs = 0.0
        seqnum = 1
        generator_config["Perturbation_Experiment"] = {}
        for num_nodes in num_nodes_list:
            max_wasted_ncores_frac = 0.1 if num_nodes <= 1 else 0.05 if num_nodes <=3 else 0.02
            layout_config = LayoutSearchConfig(tol_around_ctrl_ratio=tol_around_ctrl_ratio, max_wasted_ncores_frac=max_wasted_ncores_frac)
            layout = generate_esm1p6_core_layouts_from_node_count(
                num_nodes,
                queue=queue,
                layout_search_config=layout_config,
            )[0]
            if not layout:
                logger.warning(f"No layouts found for {num_nodes} nodes")
                continue

            layout = [x for x in layout if x not in seen_layouts]
            seen_layouts.update(layout)
            logger.info(f"Generated {len(layout)} layouts for {num_nodes} nodes. Layouts: {layout}")

            branch_name = f"{branch_name_prefix}-unused-cores-to-cice-{layout_config.allocate_unused_cores_to_ice}"
            block, seqnum = generate_esm1p6_perturb_block(
                num_nodes, layout, branch_name, queue=queue, start_seqnum=seqnum,
            )
            nblocks_added = len(block.keys())
            walltime_hrs += nblocks_added * (1.5 * 4.0/num_nodes) # use a 1.5 hrs time for 4-node runs, and then scale linearly
            generator_config["Perturbation_Experiment"].update(block)

        generator_config["Control_Experiment"]["config.yaml"]["walltime"] = f"{int(walltime_hrs)}:00:00"

        expgen = ExperimentGenerator(generator_config)
        expgen.run()


class RAM3Profiling(CylcRoseManager):
    """Handles profiling of ACCESS-rAM3 configurations."""

    def __init__(self, work_dir: Path, archive_dir: Path, layout_variable: str):
        super().__init__(work_dir, archive_dir)
        self.layout_variable = layout_variable

    def parse_ncpus(self, path: Path) -> int:
        # this is a symlink
        config_path = path / "log/rose-suite-run.conf"

        if not config_path.is_file():
            raise FileNotFoundError(f"Could not find suitable config file in {config_path}")

        for line in config_path.read_text().split():
            if not line.startswith("!!"):
                keypair = line.split("=")
                if keypair[0].strip() == self.layout_variable:
                    layout = keypair[1].split(",")
                    return int(layout[0].strip()) * int(layout[1].strip())

        raise ValueError(f"Cannot find layout key, {self.layout_variable}, in {config_path}.")

    @property
    def known_parsers(self):
        return {
            "UM_regions": UMProfilingParser(),
            "UM_total": UMTotalRuntimeParser(),
        }

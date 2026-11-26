# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Profiling studies of applications driven by a Cylc Rose suite, such as the ACCESS models ACCESS-AM3 and ACCESS-rAM3.

`CylcRoseManager` is the manager for every rose suite: it generates experiments as copies of the control suite, with
the changes of each written into its files, runs them with `rose suite-run`, and reads back what the suite and Cylc
recorded about each run - the CPUs its jobs occupied, the state its tasks reached and the profiling logs they wrote.
A suite prepared by hand is added with `add_rose_experiment` instead. What is being profiled arrives as a
`RoseSuiteConfiguration`.

The changes of an experiment are keyed by the file of the suite they change, relative to the suite directory, and
take one of two shapes. A Rose configuration, whose name ends in `.conf`, is read and written with `RoseConfParser`,
and takes the settings of each section. Any other file, such as a `suite.rc` or a site file, is a text the suite's
own tools read in their own ways, so it takes texts to replace.
"""

import hashlib
import logging
import re
import shutil
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

from access.config import RoseConfParser
from access.config.parallel_component import ComponentLayout

from access.profiling.control import ExistingDirectoryControlSource
from access.profiling.cylc_parser import CylcDBReader, CylcProfilingParser
from access.profiling.experiment import (
    ExperimentPlan,
    ProfilingExperiment,
    ProfilingExperimentStatus,
    ProfilingLog,
)
from access.profiling.manager import ProfilingManager
from access.profiling.rose_configuration import SUITE_CONF, SUITE_VARIABLES, RoseSuiteConfiguration

logger = logging.getLogger(__name__)

# The PBS request for CPUs in a job script, as Cylc writes the directive - `#PBS -l ncpus=576` - possibly after
# other resources in the same list.
_PBS_NCPUS = re.compile(r"^#PBS\s+-l\s+(?:[^\s,]+,)*ncpus=(?P<ncpus>\d+)\b", re.MULTILINE)

# What the name of an application may be, since it begins the name of every suite of a study, and so the name of the
# directory the suite is run from: letters, digits, '_', '.' and '-', beginning with a letter or a digit.
_SUITE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


def _suite_name(application_name: str, work_dir: Path, experiment: str) -> str:
    """Returns the name of the suite an experiment of a study runs as.

    `rose suite-run` names a suite after the directory it is run from, and Cylc runs every suite of a user in
    `~/cylc-run/<name>`. The name of an experiment alone, `exp-0001` in every study, would have two studies run in
    one directory. The name is therefore made of the application's, a digest of the working directory and the
    experiment's, as the Payu laboratory of a study is named: the same in every session of the study, and different
    for another one.

    Args:
        application_name (str): The name of the application being profiled.
        work_dir (Path): The working directory of the study.
        experiment (str): The name of the experiment.

    Returns:
        str: The name, `<application name>-<8 hex digits>-<experiment>`.
    """
    digest = hashlib.sha256(str(work_dir.resolve()).encode()).hexdigest()[:8]
    return f"{application_name}-{digest}-{experiment}"


def _walltime_duration(hours: float) -> str:
    """Returns a walltime in hours as the value of a suite variable stating it: an ISO 8601 duration, quoted.

    The variables of `[jinja2:suite.rc]` are Jinja2 values, so a duration is a string with its quotes, as in
    `MAIN_CLOCK='PT1H10M'`. The seconds are rounded, as for Payu, and written only where there are some.

    Args:
        hours (float): Walltime to request, in hours.

    Returns:
        str: The value, such as `'PT24H00M'`, or `'PT0H07M24S'` for 0.12345 hours.
    """
    hours_part, remainder = divmod(round(hours * 3600), 3600)
    minutes, seconds = divmod(remainder, 60)
    duration = f"PT{hours_part}H{minutes:02d}M" + (f"{seconds:02d}S" if seconds else "")
    return f"'{duration}'"


def _source_file(directory: Path, file: str) -> Path:
    """Returns the file of a suite that a change names, refusing one whose writing would reach outside the suite.

    The file may itself be a symbolic link, as `rose-suite.conf` is in the suites of ACCESS-AM3: it is read through
    the link, and `_create_experiments` writes the changed file in place of the link. A symbolic link to a directory
    on the way is refused instead. A copy of a suite keeps it as a link, so the file reached through it may be outside
    the copy, and writing it would change whatever the link leads to, the control included.

    Args:
        directory (Path): The suite directory.
        file (str): The file, relative to the suite directory.

    Returns:
        Path: The file, which may be a symbolic link.

    Raises:
        ValueError: If the path is absolute, leads out of the suite through `..`, passes through a symbolic link to
            a directory, or is not a file the suite has.
    """
    relative = Path(file)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(
            f"The changes name the file {file!r}, which is not inside the suite. Name a file of the suite relative to "
            "its directory, such as 'rose-suite.conf'."
        )
    path = directory
    for part in relative.parts[:-1]:
        path = path / part
        if path.is_symlink():
            raise ValueError(
                f"The changes name the file {file!r}, which is reached through the symbolic link {path} to a "
                "directory. Writing it would change what the link leads to, which may be outside the experiment's "
                "copy of the suite. Name the file through the directory the link leads to, where it is in the suite."
            )
    path = directory / relative
    if not path.is_file():
        raise ValueError(f"The changes name the file {file!r}, which the suite {directory} does not have.")
    return path


def _set_setting(settings: dict, key: str, value: object, where: str) -> None:
    """Sets one setting of a Rose configuration, enabling it where the file ignores it.

    Args:
        settings (dict): The settings of the section, or of no section, as `RoseConfParser` reads them.
        key (str): The setting.
        value (object): Its value, which has to be one line of text with nothing to trim.
        where (str): The section and the file, for a message.

    Raises:
        ValueError: If the value is not one line of text with nothing to trim, or if the key cannot be a setting
            there, as a section's name cannot.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"The changes set {key} in {where} to {value!r}, which is not a string. A Rose setting is given as the "
            "file is to state it, such as '16', or \"'PT2H'\" for a Jinja2 string."
        )
    # A setting the file ignores, `!KEY` or `!!KEY`, is enabled, as `rose suite-run -D` overrides it. It cannot be
    # given its value in place, since its marks are part of its key, and neither can a setting with nothing after its
    # `=`, whose value has another type: both are written anew, at the end of the section.
    replaced = [f"!{key}", f"!!{key}"]
    if key in settings and settings[key] is None:
        replaced.append(key)
    for old in replaced:
        if old in settings:
            del settings[old]
    try:
        settings[key] = value
    except (TypeError, ValueError) as error:
        raise ValueError(f"Cannot set {key} in {where} to {value!r}: {error}") from error


def _edit_conf(text: str, layers: list[dict], file: str) -> str:
    """Returns the text of a Rose configuration with the changes of each layer made to it, one layer after another.

    A layer gives the settings of each section, `{section: {setting: value}}`, with the settings of no section under
    `""`. A value is a string, written as given: a Jinja2 string carries its quotes. A setting the section lacks is
    added at its end, and so is a setting the file ignores, which is enabled. The rest of the file is kept as it is,
    comments included.

    Args:
        text (str): The file.
        layers (list[dict]): The changes, in the order they are made.
        file (str): The file's path in the suite, for a message.

    Returns:
        str: The file with the changes made.

    Raises:
        ValueError: If a layer is not of that shape, if the file has no section a layer names, other than one it
            ignores, or if a value is not one line of text with nothing to trim.
    """
    conf = RoseConfParser().parse(text)
    for layer in layers:
        for section, settings in layer.items():
            if not isinstance(settings, dict):
                raise ValueError(
                    f"The changes to {file} give {settings!r} for {section!r}. A Rose configuration takes the "
                    "settings of each section, {section: {setting: value}}, with those of no section under ''."
                )
            target = conf if section == "" else conf.get(section)
            if not isinstance(target, dict):
                raise ValueError(
                    f"{file} has no section [{section}] in use, so its settings cannot be changed. Name a section the "
                    "file has, without the brackets, such as 'jinja2:suite.rc'."
                )
            where = f"[{section}] of {file}" if section else file
            for key, value in settings.items():
                _set_setting(target, key, value, where)
    return str(conf)


def _replace_text(text: str, layers: list[dict], file: str) -> str:
    """Returns the text of a file with the replacements of each layer made in it, one layer after another.

    A layer maps a text to the text replacing it, `{old text: new text}`, and every occurrence is replaced, in the
    text the replacements before it left. A text that does not occur is refused rather than passed over: the change
    would otherwise be lost without a word, as where the control writes the text in another way.

    Args:
        text (str): The file.
        layers (list[dict]): The replacements, in the order they are made.
        file (str): The file's path in the suite, for a message.

    Returns:
        str: The file with the replacements made.

    Raises:
        ValueError: If a replacement is not a string, if a text to replace is empty, or if it does not occur.
    """
    for layer in layers:
        for old, new in layer.items():
            if not isinstance(new, str):
                raise ValueError(
                    f"The changes to {file} replace {old!r} by {new!r}, which is not a string. A file other than a "
                    "Rose configuration takes texts to replace, {old text: new text}."
                )
            if not old:
                raise ValueError(
                    f"The changes to {file} replace an empty text, which occurs between every two characters. Give "
                    "the text to replace."
                )
            if old not in text:
                raise ValueError(
                    f"The changes to {file} replace {old!r}, which does not occur in it. Give the text as the control "
                    "writes it, after any change made before this one."
                )
            text = text.replace(old, new)
    return text


class CylcRoseManager(ProfilingManager[RoseSuiteConfiguration]):
    """Profiling of any application driven by a Cylc Rose suite, such as the ACCESS models.

    One class for every rose suite: what tells ACCESS-AM3 from ACCESS-rAM3 is the components they run, the
    variables of their `rose-suite.conf` that state the parallelism of each, and which parsers read their task
    logs, all of which arrive as a RoseSuiteConfiguration. What is written here is the engine - the copies of the
    control suite, `rose suite-run`, the Cylc run directory and the suite database the scheduler writes - and none
    of it is any one suite's.

    Args:
        work_dir (Path): Working directory where profiling experiments will be generated and run.
        archive_dir (Path): Directory where completed experiments will be archived.
        application (RoseSuiteConfiguration): The suite being profiled.
        control (ControlSource | None): Where the control suite comes from, which every generated experiment is a
            copy of. Nothing here clones or checks it out: it is expected at the path of an
            `ExistingDirectoryControlSource`, and otherwise at `<work_dir>/<control.directory>`, `git clone` or
            `rosie checkout` having been run in a terminal, where a credentials prompt can be answered. None (the
            default) is enough to add, read and plot suites, but not to generate any.
    """

    def _control_directory(self) -> Path:
        """Returns the directory of the control suite, which every experiment is a copy of.

        Returns:
            Path: The path of an `ExistingDirectoryControlSource`, and otherwise `<work_dir>/<control.directory>`.

        Raises:
            ValueError: If this manager has no control, or if the directory has no `rose-suite.conf`.
        """
        # Bound once, and checked here: generating refuses a missing control before it gets this far, but the hooks
        # that call this can be called on their own, and `control` is a settable property.
        control = self.control
        if control is None:
            raise ValueError(
                "Cannot create experiments without a control: every experiment is a copy of the control suite, so "
                "there is nothing to copy. Pass a ControlSource to this manager."
            )
        if isinstance(control, ExistingDirectoryControlSource):
            directory = control.path
        else:
            directory = self.work_dir / control.directory
        if not (directory / SUITE_CONF).is_file():
            revision = "" if control.start_point is None else f" at {control.start_point}"
            raise ValueError(
                f"The control suite is not in {directory}: there is no {SUITE_CONF} there. Nothing here fetches a "
                f"suite, so check out {control.origin}{revision} into {directory} first, with `git clone` or `rosie "
                "checkout`, and try again."
            )
        return directory

    def _edits(
        self, control_dir: Path, plans: list[ExperimentPlan], control_options: dict | None
    ) -> dict[str, dict[str, str]]:
        """Returns the text of every file each planned experiment changes, as it is to be written, writing nothing.

        This is both what `_refuse_plans` checks and what `_create_experiments` writes, so that the two cannot
        disagree. The changes are made in layers, each over the one before: the control options, the plan's changes,
        and its walltime. A file the same changes are made to for several experiments is edited once, since reading
        a large Rose configuration takes seconds. A file named by a symbolic link is read through it, and its text
        is keyed by the name given, not by the file the link leads to.

        Args:
            control_dir (Path): The directory of the control suite.
            plans (list[ExperimentPlan]): The experiments to create.
            control_options (dict | None): The changes made to every experiment before its own, or None.

        Returns:
            dict[str, dict[str, str]]: The text of each file, keyed by experiment and then by the file's path relative
                to the suite directory.

        Raises:
            ValueError: If the application's name cannot begin the name of a suite, if a plan states a walltime and
                the application no `walltime_variable`, or if a change cannot be made.
        """
        if _SUITE_NAME.fullmatch(self.application.name) is None:
            raise ValueError(
                f"The name of the application, {self.application.name!r}, cannot begin the name of a suite, which is "
                "the name of the directory the suite is run from. Name the application with letters, digits, '_', "
                "'.' and '-', beginning with a letter or a digit."
            )
        walltime_variable = self.application.walltime_variable
        texts: dict[tuple[Path, str], str] = {}
        edits = {}
        for plan in plans:
            layers = [control_options or {}, plan.changes]
            if plan.walltime_hours is not None:
                if walltime_variable is None:
                    raise ValueError(
                        f"{self.application.name} states its walltime in no variable of {SUITE_CONF}, so a walltime "
                        "cannot be written for it. Generate the experiments with generate_experiments instead, with "
                        "the layouts select_layouts returns and the walltime among the changes, in the file the "
                        "suite states it in."
                    )
                walltime = _walltime_duration(plan.walltime_hours)
                layers.append({SUITE_CONF: {SUITE_VARIABLES: {walltime_variable: walltime}}})
            # The Rose configurations of an experiment being regenerated are edited where it has them, so that a
            # correction made there by hand is kept, as on a Payu branch. Its other files are taken from the control
            # again, since replacing a text a second time can apply it twice: 3600 would become 36000, then 360000.
            conf_dir = self.experiments[plan.name].path if plan.regenerating else control_dir
            files = {}
            for file in dict.fromkeys(file for layer in layers for file in layer):
                changes = [layer[file] for layer in layers if file in layer]
                if not all(isinstance(change, dict) for change in changes):
                    raise ValueError(
                        f"The changes to {file} are not a mapping: a Rose configuration takes {{section: {{setting: "
                        "value}}, and any other file {old text: new text}."
                    )
                is_conf = Path(file).suffix == ".conf"
                source = _source_file(conf_dir if is_conf else control_dir, file)
                key = (source, repr(changes))
                if key not in texts:
                    texts[key] = (_edit_conf if is_conf else _replace_text)(source.read_text(), changes, file)
                files[file] = texts[key]
            edits[plan.name] = files
        return edits

    def _refuse_plans(self, plans: list[ExperimentPlan], control_options: dict | None = None, **runner_options) -> None:
        """Refuses the planned experiments whose changes could not be written into a copy of the control suite.

        Everything `_create_experiments` would write is worked out, and nothing is written, so that a call refused
        here takes no number.

        Args:
            plans (list[ExperimentPlan]): The experiments about to be created.
            control_options (dict | None): The changes made to every experiment before its own. None (the default)
                makes none.
            **runner_options: Ignored. Accepted so that this stays substitutable for the base method, whose
                callers may pass options other runners take.

        Raises:
            ValueError: For the reasons `_create_experiments` gives.
        """
        self._edits(self._control_directory(), plans, control_options)

    def _create_experiments(
        self, plans: list[ExperimentPlan], control_options: dict | None = None, **runner_options
    ) -> dict[str, ProfilingExperiment]:
        """Creates the planned experiments as copies of the control suite, with their changes written into their files.

        A new experiment is a copy of the control suite at `<work_dir>/<experiment>/<suite name>`, as `_suite_name`
        names it, without the version control directories `.git` and `.svn`, and with its symbolic links kept as
        links. A file the changes name is the exception: where it is a link, the copy has the changed file in its
        place, with the permissions of the file the control's link leads to. Its run directory is
        `~/cylc-run/<suite name>`, where `rose suite-run` run in the copy installs the suite. A plan being regenerated
        is written into the copy the experiment has already.

        The changes are the engine's words, keyed by the file of the suite they change, relative to its directory:

        - a Rose configuration, whose name ends in `.conf`, takes the settings of each section, `{section: {setting:
          value}}`, with those of no section under `""`. A value is a string, written as the file is to state it,
          so a Jinja2 string carries its quotes. A setting the section lacks is added, and one the file ignores is
          enabled. A section the file lacks is refused.
        - any other file takes texts to replace, `{old text: new text}`. Every occurrence is replaced, and a text
          that does not occur is refused.

        The control options are made first, then the plan's changes, and then its walltime, which is written into the
        application's `walltime_variable` as an ISO 8601 duration.

        Args:
            plans (list[ExperimentPlan]): The experiments to create.
            control_options (dict | None): The changes made to every experiment before its own, in the same shapes.
                None (the default) makes none.
            **runner_options: Ignored. Accepted so that this stays substitutable for the base method, whose
                callers may pass options other runners take.

        Returns:
            dict[str, ProfilingExperiment]: The experiments new to this manager, keyed by name, each with its suite
                directory, its run directory and the layout of its plan.

        Raises:
            ValueError: If this manager has no control, if the control suite is not in its directory, if the
                application's name cannot begin the name of a suite, if a plan states a walltime and the application
                no `walltime_variable`, or if a change cannot be made: a file it names is not a file of the suite,
                or is reached through a symbolic link to a directory, a section it names is not in the file, a value
                is not one line of text, or a text to replace does not occur.
            OSError: If a copy cannot be made, as where its directory exists already.
        """
        control_dir = self._control_directory()
        edits = self._edits(control_dir, plans, control_options)
        created = {}
        for plan in plans:
            if plan.regenerating:
                suite_dir = self.experiments[plan.name].path
            else:
                suite_dir = self.work_dir / plan.name / _suite_name(self.application.name, self.work_dir, plan.name)
                shutil.copytree(control_dir, suite_dir, symlinks=True, ignore=shutil.ignore_patterns(".git", ".svn"))
                run_path = Path.home() / "cylc-run" / suite_dir.name
                created[plan.name] = ProfilingExperiment(path=suite_dir, run_path=run_path, layout=plan.layout)
                logger.info(f"Created experiment {plan.name} as the suite '{suite_dir}', to run in '{run_path}'.")
            for file, text in edits[plan.name].items():
                path = suite_dir / file
                if path.is_symlink():
                    # Writing through the link would change what it leads to, which may be outside the copy, as an
                    # absolute link leads to the control's own file. The permissions are taken through the control's
                    # link, since a relative link that leads out of the suite leads elsewhere from the copy.
                    path.unlink()
                    path.write_text(text)
                    shutil.copymode(control_dir / file, path)
                else:
                    # Written in place, which keeps the permissions of the file.
                    path.write_text(text)
        return created

    def parse_ncpus(self, path: Path, run_path: Path | None = None) -> int:
        """Parses the number of CPUs a given Cylc/Rose experiment occupied.

        The layout is read first: finding the configuration file is this manager's, since where a rose suite
        keeps it and how it is written is the runner's business, while what its variables are called, and so how
        many CPUs they come to, is the suite's and is answered by the configuration.

        The job that runs the layout may occupy more CPUs than the layout puts to work, to fill whole compute
        nodes, and the scheduler charges for those. So, as PayuManager.parse_ncpus reads what the scheduler was
        asked for, this reads the PBS requests in the job scripts Cylc submitted, and reports the smallest that
        covers the layout: that of the job that ran it. Where no job script covers the layout - no run directory,
        no job submitted to PBS - the layout's own count is reported. See _requested_ncpus.

        Args:
            path (Path): Path to the experiment directory. Must contain a rose-suite.conf file.
            run_path (Path | None): Optional path to a separate runs directory. Its run configuration is
                preferred over the one in path when present, since it records what the run actually used, and
                its job scripts say what the run's jobs asked PBS for.

        Returns:
            int: Number of CPUs the experiment occupied, including any left idle to fill whole compute nodes.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If the file that was found has no `[jinja2:suite.rc]` section, or if a variable the
                configuration names is not set in it.
        """
        layout_cpus = self._layout_ncpus(path, run_path)
        if run_path is None:
            return layout_cpus

        covering = [ncpus for ncpus in self._requested_ncpus(run_path) if ncpus >= layout_cpus]
        if not covering:
            logger.debug(f"No job in {run_path} asked PBS for the {layout_cpus} CPUs of the layout, or more.")
            return layout_cpus
        return min(covering)

    @staticmethod
    def _requested_ncpus(run_path: Path) -> list[int]:
        """Returns the numbers of CPUs the jobs of a suite's run asked PBS for.

        Cylc writes the directives of a task's job into the script it submits, at
        log/job/<cycle>/<task>/NN/job for the latest attempt at it, so a job submitted to PBS has a `#PBS -l
        ncpus=<n>` line there. A job asking for chunks with `select` instead, or not submitted to PBS, says
        nothing here.

        Args:
            run_path (Path): Path to the Cylc run directory.

        Returns:
            list[int]: The request of every job script that makes one, in no particular order.
        """
        requests = []
        for script in run_path.glob("log/job/*/*/NN/job"):
            match = _PBS_NCPUS.search(script.read_text(errors="replace"))
            if match is not None:
                requests.append(int(match["ncpus"]))
        return requests

    def _layout_ncpus(self, path: Path, run_path: Path | None) -> int:
        """Returns the number of CPUs the layout of a suite puts to work, from its rose-suite.conf.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory, whose run configuration is
                preferred.

        Returns:
            int: Number of CPUs the layout puts to work.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If the file that was found has no `[jinja2:suite.rc]` section, or if a variable the
                configuration names is not set in it.
        """
        return self.application.occupied_cpus(self._suite_conf(path, run_path))

    def _parse_layout(self, path: Path, run_path: Path | None) -> ComponentLayout | None:
        """Returns the layout a suite ran, from the same file as its CPU count.

        That is the configuration the run recorded, where there is one, since it says what the run used, with the
        values `rose suite-run` was given and the optional configurations it applied. Otherwise it is the suite's
        own `rose-suite.conf`. The layout and the CPU count of a suite therefore never disagree.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory, whose run configuration is
                preferred.

        Returns:
            ComponentLayout | None: The layout, or None if the variables give no component any cores.

        Raises:
            FileNotFoundError: If neither configuration file exists.
            ValueError: If the file that was found has no `[jinja2:suite.rc]` section, or if a variable the
                configuration names is not set in it.
        """
        return self.application.read_layout(self._suite_conf(path, run_path))

    @staticmethod
    def _suite_conf(path: Path, run_path: Path | None) -> Path:
        """Returns the file the variables of a suite are read from.

        Both the run and the suite state them: `log/rose-suite-run.conf`, which `rose suite-run` writes into the run
        directory, is preferred, being what the run used.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Optional path to a separate runs directory.

        Returns:
            Path: The run's configuration, if it is there, or else the suite's `rose-suite.conf`.

        Raises:
            FileNotFoundError: If neither configuration file exists.
        """
        config_paths = []
        if run_path is not None:
            config_paths.append(run_path / "log/rose-suite-run.conf")
        config_paths.append(path / SUITE_CONF)

        config_path = next((candidate for candidate in config_paths if candidate.is_file()), None)
        if config_path is None:
            tried = ", ".join(str(p) for p in config_paths)
            raise FileNotFoundError(f"Could not find suitable config file. Tried: {tried}")
        return config_path

    def add_rose_experiment(self, rose: str, run_path: Path | None = None, labels: dict | None = None) -> None:
        """Adds the given rose as an experiment to this manager.

        The experiment is named after the rose, and gets a record in its directory, from which a manager built
        later finds it again.

        The experiment takes the status the suite database in its run directory records, as parse_status reads
        it: DONE once every task has succeeded, FAILED once one has not, and RUNNING while some are still going. A
        suite with no run recorded there is NEW, which is what run_experiments runs.

        Args:
            rose (str): The rose to add as an experiment.
            run_path (Path | None): Path to the Cylc run directory of the suite, where its results are, or will be
                once it runs: it does not have to exist yet. Without it, nothing the suite records can be read, so
                the suite stays NEW, a run of it is never seen to finish, and its logs cannot be parsed.
            labels (dict | None): The study's own keys and values for this experiment, as JSON values. None (the
                default) gives it none.

        Raises:
            ValueError: If the rose is not the name of a single directory or is named as generated experiments are,
                `exp-<digits>`, if the experiment path does not exist, or if the labels are not JSON values. Nothing
                is added when it is raised.
        """
        self._refuse_added_name(rose)
        experiment_path = self.work_dir / rose
        if not experiment_path.is_dir():
            raise ValueError(f"Experiment path '{experiment_path}' does not exist or is not a directory.")

        experiment = ProfilingExperiment(path=experiment_path, run_path=run_path, labels=labels)
        status = self.parse_status(experiment_path, run_path)
        experiment.status = status if status is not None else ProfilingExperimentStatus.NEW
        self.experiments[rose] = experiment
        self._write_record(rose)
        self._log("add_rose_experiment", [rose])

    def run_experiments(self, retry_failed: bool = False) -> None:
        """Runs Rose Cylc experiments via `rose suite-run` for profiling data generation.

        An experiment whose suite failed is run where retry_failed asks for it, with `rose suite-run --new`. This
        removes its run directory first, as `rose suite-clean` does, logs and database included, so that the suite
        starts over and its status is that of the new run.

        Args:
            retry_failed (bool): Whether to run the experiments whose suites failed as well as the new ones.
                False (the default) runs only the new ones.
        """

        # An experiment is only new as far as this manager knows: a status does not outlive the session, so
        # one generated and run in an earlier session comes back NEW. Ask before deciding what to submit.
        self.update_statuses()

        wanted = {ProfilingExperimentStatus.NEW}
        if retry_failed:
            wanted.add(ProfilingExperimentStatus.FAILED)

        to_run = {name: exp for name, exp in self.experiments.items() if exp.status in wanted}

        if not to_run:
            logger.info("No new experiments to run. Will skip execution.")
            return

        for name, exp in to_run.items():
            # The database of a failed run records the task that failed, and a run over it would still read as
            # failed.
            command = ["rose", "suite-run"]
            if exp.status == ProfilingExperimentStatus.FAILED:
                command.append("--new")
            logger.info(f"Running experiment '{name}' via {' '.join(command)} in '{exp.path}'.")
            try:
                result = subprocess.run(command, cwd=exp.path, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                for line in e.stdout.splitlines():
                    logger.info(f"[{name}] {line}")
                for line in e.stderr.splitlines():
                    logger.error(f"[{name}] {line}")
                raise
            for line in result.stdout.splitlines():
                logger.info(f"[{name}] {line}")
            for line in result.stderr.splitlines():
                logger.warning(f"[{name}] {line}")
            exp.status = ProfilingExperimentStatus.RUNNING

    def _delete_experiments(self, names: list[str], dry_run: bool, **kwargs) -> None:
        """Deletes the experiment and run directories of Rose Cylc experiments.

        A run directory that is a symbolic link, as `rose suite-run` makes it where the suite states a `root-dir`, is
        deleted where the link leads, and then the link.

        Args:
            names (list[str]): Names of the experiments to delete.
            dry_run (bool): If True, logs what would be deleted without making any changes.
            **kwargs: Ignored. Accepted so that this stays substitutable for the base method, whose callers
                may pass options other runners take.
        """
        for name in names:
            exp = self.experiments[name]
            exp_path = exp.path
            run_path = exp.run_path
            if dry_run:
                logger.info(f"Dry run: would delete experiment directory '{exp_path}' and run directory '{run_path}'.")
                continue
            if exp_path.is_dir():
                logger.info(f"Deleting experiment directory '{exp_path}'.")
                shutil.rmtree(exp_path)
            else:
                logger.warning(f"Experiment directory '{exp_path}' does not exist. Skipping deletion.")
            if run_path is not None:
                if run_path.is_symlink():
                    target = run_path.resolve()
                    if target.is_dir():
                        logger.info(f"Deleting run directory '{target}', which '{run_path}' links to.")
                        shutil.rmtree(target)
                    logger.info(f"Deleting the link '{run_path}'.")
                    run_path.unlink()
                elif run_path.is_dir():
                    logger.info(f"Deleting run directory '{run_path}'.")
                    shutil.rmtree(run_path)
                else:
                    logger.warning(f"Run directory '{run_path}' does not exist. Skipping deletion.")

    def archive_experiments(
        self,
        exclude_dirs: list[str] | None = None,
        exclude_files: list[str] | None = None,
        follow_symlinks: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Archives completed experiments to the specified archive path.

        Args:
            exclude_dirs (list[str] | None): Directory patterns to exclude when archiving. Defaults to
                [".svn", "share"] if not provided.
            exclude_files (list[str] | None): File patterns to exclude when archiving. Defaults to
                ["*.nc"] if not provided.
            follow_symlinks (bool): Whether to follow symlinks when archiving. Defaults to False.
            overwrite (bool): Whether to overwrite existing archives. Defaults to False.
        """
        if exclude_dirs is None:
            exclude_dirs = [".svn", "share"]
        if exclude_files is None:
            exclude_files = ["*.nc"]
        super().archive_experiments(
            exclude_dirs=exclude_dirs,
            exclude_files=exclude_files,
            follow_symlinks=follow_symlinks,
            overwrite=overwrite,
        )

    def parse_status(self, path: Path, run_path: Path | None = None) -> ProfilingExperimentStatus | None:
        """Parses the state the tasks of a Rose Cylc suite reached.

        Cylc records the state of each task in the task_states table of the suite database, and a run status per
        attempt at it in the task_jobs table: 0 where the attempt succeeded, non-zero where it failed, and nothing
        while it is still going or where its job could not be submitted. A suite has failed as soon as Cylc gives up
        on one of its tasks, whose state is then `failed` or `submit-failed`. It is done once the latest attempt at
        every task it has recorded has succeeded, and still running otherwise. An attempt that failed therefore fails
        the suite only where Cylc does not run the task again, as it does where the suite sets `execution retry
        delays`, and not at all where a later attempt succeeds.

        Args:
            path (Path): Path to the experiment directory. Unused: the database lives with the run.
            run_path (Path | None): Path to the runs directory, which holds cylc-suite.db.

        Returns:
            ProfilingExperimentStatus | None: The state of the suite, or None if the database says nothing
                readable - no run directory, no database yet, or no task recorded in it.
        """
        if run_path is None:
            logger.debug("No Cylc run directory, so there is no suite database to read a status from.")
            return None

        cylcdb = run_path / "cylc-suite.db"
        if not cylcdb.is_file():
            logger.debug(f"No Cylc suite database at {cylcdb}.")
            return None

        # A connection used as a context manager only ends a transaction, so it is closed explicitly.
        try:
            with closing(sqlite3.connect(f"file:{cylcdb}?mode=ro", uri=True)) as connection:
                states = {row[0] for row in connection.execute("SELECT status FROM task_states")}
                # SQLite takes a column that is not aggregated from the row holding the maximum: the latest attempt.
                latest = "SELECT run_status, MAX(submit_num) FROM task_jobs GROUP BY cycle, name"
                statuses = [row[0] for row in connection.execute(latest)]
        except sqlite3.Error as error:
            logger.debug(f"Could not read task statuses from {cylcdb}: {error}.")
            return None

        if not statuses:
            logger.debug(f"The Cylc suite database at {cylcdb} records no task yet.")
            return None

        if states & {"failed", "submit-failed"}:
            return ProfilingExperimentStatus.FAILED
        if any(status != 0 for status in statuses):
            return ProfilingExperimentStatus.RUNNING
        return ProfilingExperimentStatus.DONE

    def profiling_logs(self, path: Path, run_path: Path | None = None) -> dict[str, dict[int, ProfilingLog]]:
        """Returns all profiling logs from the specified path.

        Args:
            path (Path): Path to the experiment directory.
            run_path (Path | None): Path to the Cylc run directory.

        Returns:
            dict[str, dict[int, ProfilingLog]]: Dictionary mapping log names to their logs, keyed by run number.
                Cylc workflows have no concept of repeated runs, so every log is returned as the single run 0.

        Raises:
            ValueError: If no run directory is given.
            RuntimeError: If the run directory holds no task job logs.
        """
        if run_path is None:
            raise ValueError("Cylc run_path is required to locate profiling logs.")

        logs = {}

        # setup log paths
        suite_log = run_path / "log/suite/log"  # cylc log file
        cylcdb = run_path / "cylc-suite.db"  # database with task runtimes
        jobdir = run_path / "log/job"  # where task logs are stored

        logs["cylc_suite_log"] = ProfilingLog(suite_log, CylcProfilingParser())
        # cylcdb.read_text = lambda x: x # hack to make log work
        logs["cylc_tasks"] = ProfilingLog(cylcdb, CylcDBReader())

        # Search for available profiling logs for the components in the configuration.
        # matches <cycle> / <task> / NN / job.out e.g. 20220226T0000Z/Lismore_d1100_GAL9_um_fcst_000/NN/job.out
        # NN is the last attempt
        # job.out is the stdout
        # this pattern is followed for all cylc workflows.
        # as tasks of interest will likely have their own logging regions e.g. UM each task_cycle is
        # treated as a "component" of the configuration.
        possible_component_logs = list(jobdir.glob("*/*/NN/job.out"))
        if not possible_component_logs:
            raise RuntimeError(f"Could not find any known logs in {jobdir}")

        for logfile in possible_component_logs:
            cycle, task = logfile.parts[-4:-2]
            for parser_name, parser in self.application.parsers.items():
                logs[f"{task}_cycle{cycle}_{parser_name}"] = ProfilingLog(logfile, parser, optional=True)

        # Cylc workflows have no concept of repeated runs, so every log is registered as the single run 0.
        return {name: {0: log} for name, log in logs.items()}

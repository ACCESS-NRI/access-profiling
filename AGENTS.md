# AGENTS.md

Guidance for AI coding agents working in `access-profiling`.

## What this project is

A Python library from ACCESS-NRI for profiling parallel applications. A study generates one experiment per parallel
layout over a range of core counts, runs them through a workflow engine, parses the profiling logs they write into
`xr.Dataset`s quantified with pint, and selects and plots the best layouts. The package is generic in both the
application and the engine, and ships implementations for the ACCESS climate models: ACCESS-ESM1.6 and ACCESS-OM3
run with Payu, and UM suites run with Cylc and Rose.

Python >= 3.11.4, which the `filter="data"` tarball extraction in `experiment.py` needs; CI covers 3.11–3.13. The
layout search (`ParallelComponent`, `iter_layouts`, the allocation strategies and the constraints) and the parsers of
configuration files (`YAMLParser`, `NUOPCParser`, ...) come from
[access-config-utils](https://github.com/ACCESS-NRI/access-config-utils). The Payu manager creates experiments with
experiment-generator and runs them with experiment-runner.

The package is built on two axes. A **manager** (`ProfilingManager`) drives one workflow engine: it generates,
runs, archives and parses experiments, and reads what the engine and the scheduler wrote. An **application**
(`Application`) describes what is profiled: its component tree, how a layout is written into its input files and
read back out, and which logs it writes. A study is a manager of one engine handed one application and a control.

**Read the [developer guide](documentation/docs/pages/developer-guide/index.md) first.** It describes the two axes
at length, the abstract classes and what a subclass of each implements, and how a study is generated, and the
module docstrings say why the code is the way it is. This file does not repeat them; it adds what an agent changing
the code needs on top of them.

## Working here

- **Think before coding.** State your assumptions; where a request is ambiguous, ask rather than pick silently.
  Point out a simpler alternative or a tradeoff, and push back when it is warranted.
- **Simplicity first.** Write the least code that solves the problem: no speculative features, single-use
  abstractions, unrequested configurability, or handling of errors that cannot happen. A hook nothing overrides is
  removed, not kept for later, and an unreachable branch could not be covered anyway.
- **Exact over approximate.** Core counts are integers and `ncpus` is what the job was charged for, a parser raises
  on a log it cannot read rather than returning what it could, and a value keeps its pint units. Don't trade any of
  that for a shortcut.
- **Surgical changes.** Every changed line traces to the request. Match the surrounding style; don't refactor,
  reformat or "improve" adjacent code. Remove what your change orphans; mention pre-existing dead code rather than
  deleting it.
- **The documentation follows the code.** The docstrings, the [user guide](documentation/docs/pages/user-guide/) and
  the [ACCESS models](documentation/docs/pages/access/) pages describe the behaviour in detail, so a change of
  behaviour updates the docstrings and pages that describe it, in the same change: grep `src/` and
  `documentation/docs/pages` for the names you touched. The strict build catches a broken reference, not stale prose.
- **Be honest.** Report what you ran and what it showed: a failing test, a check you skipped, a test that passes
  without binding.

## Repository layout

```
src/access/profiling/
  ── the generic layer: no ACCESS model, no workflow engine ──
  application.py        Application (the ABC), LogSpec, log_at
  control.py            ControlSource, GitControlSource, ExistingDirectoryControlSource: where a control is
  experiment.py         ProfilingLog (one log -> one xr.Dataset), ExperimentPlan, ProfilingExperimentStatus,
                        ProfilingExperiment (archived to and extracted from a tarball)
  manager.py            ProfilingManager[AppT]: generation, statuses, parsing, selection and plotting
  parser.py             ProfilingParser (the ABC), flatten_hierarchical, aggregate_pe_data
  metrics.py            ProfilingMetric and the predefined metrics (tmin, tmax, tavg, count, ...)
  plotting_utils.py     RegionGroup, plot_bar_metrics
  scaling.py            parallel_speedup, parallel_efficiency, plot_scaling_metrics, plot_component_scaling

  ── the log parsers, one per format ──
  cice5_parser.py       CICE5ProfilingParser        (the CICE timers; OM3 reads CICE6 with it too)
  cylc_parser.py        CylcProfilingParser, CylcDBReader
  esmf_parser.py        ESMFSummaryProfilingParser  (flat or hierarchical)
  fms_parser.py         FMSProfilingParser          (the FMS clocks of MOM5 and MOM6)
  payujson_parser.py    PayuJSONProfilingParser
  um_parser.py          UMProfilingParser, UMTotalRuntimeParser, um_stdout

  ── the workflow engines ──
  payu_configuration.py PayuConfiguration, SubmodelsConfiguration, PayuComponent, PlainComponent, PayuSubmodel,
                        FixedRanksConstraint, payu_model_stdout
  payu_manager.py       PayuManager: experiment-generator, experiment-runner and Payu's job files
  rose_configuration.py RoseSuiteConfiguration
  cylc_manager.py       CylcRoseManager: rose suite-run and the Cylc suite database

  ── models/: the ACCESS models, as data ──
  cice.py               CICEPartitioning: CICE's block distribution, for ESM1.6 and OM3 alike
  components.py         UMComponent, MOM5Component, CICEComponent: the codes the Payu models are put together from
  esm16.py              ESM16Configuration, ESM16_PI_CONTROL, ESM16_AMIP and the control source
  om3.py                OM3Configuration, OM3_MC_25KM and its control source
  um_suites.py          AM3_N96E, RAM3, their control source and UM_SUITE_PARSERS

documentation/          The MkDocs site, published on Read the Docs: pages in docs/pages/, figures drawn by
                        scripts/generate_figures.py
conda/                  The conda recipe, for the accessnri channel
```

**The imports run one way, and there is no cycle**: no `TYPE_CHECKING` block and no function-local import anywhere.
Four boundaries in that graph are load-bearing:

- **The generic layer names no ACCESS model and no engine.** Whatever knows what a particular runner or code writes
  goes in a module named for it, which is why `payu_model_stdout` is in `payu_configuration` and `um_stdout` in
  `um_parser`, not in `application`.
- **Only the engine managers import `manager`.** Parsers, applications and the models never do: an application is a
  value a manager is handed, not a subclass of one.
- **`payu_configuration` does not import the job-submission stack** (`payu_manager`, experiment-generator,
  experiment-runner): describing what is profiled should not need what submits it. `__init__.py` imports
  everything, so this is about dependencies, not import time.
- **`application`, `experiment` and `parser` never import a parser module.** `um_parser` imports `application` for
  `um_stdout`, so the reverse would close a cycle.

Which side a member belongs on is decided by what it reads (the module docstring of `application.py`): **the
manager reads what the engine and the scheduler wrote; the application reads what its own input files say.**
`config_changes` writes a layout into those files and `parse_layout` reads it back out: they are inverses over the
same text, and stay side by side in every application so that they do not drift apart.

**Public names.** The names a study uses most are re-exported from `access.profiling` and listed in its `__all__`,
the only one in the package, which `test_init.py` checks against what it imports; everything else is imported
from the module that defines it, and `access.profiling.models` exports nothing. The API reference is generated
from the docstrings, on pages in `documentation/docs/pages/api/` that are each a run of `::: access.profiling.<module>`
blocks, one page per concept. A new module needs a block on the right page and a row in the table of
`api/index.md`; a new page needs an entry in the `nav` of `mkdocs.yml`, or the strict build fails.

## Invariants

### Profiling data

- **Metrics are compared by identity.** A dataset's variables are keyed by `ProfilingMetric` objects, so code looks
  one up with the module-level singleton: `tmax`, `tavg` and the rest in `metrics.py`, `pets` and `pes` in
  `esmf_parser.py`, `grain`, `pelist_min` and `pelist_max` in `fms_parser.py`. The statistics across PEs that
  `aggregate_pe_data` reports are keyed by what `pe_statistic` in `parser.py` returns, made once for each metric and
  statistic. A second `ProfilingMetric("maximum time", ...)` is another key, not an equal one.
- **`ProfilingLog.parse` builds the dataset, not the parser.** It flattens a hierarchical result, adds a `pe`
  dimension for per-PE data and quantifies each metric with its units; a parser returns the plain dict, in one of
  the three shapes [Adding a parser](documentation/docs/pages/developer-guide/adding-a-parser.md) describes, with
  every metric it declares, or `ProfilingLog.parse` raises `KeyError`. A parser raises `ValueError` on a log not
  in its format.
- **The order of `_metrics` is load-bearing** in `UMProfilingParser`, which builds its regular expression for a
  region in that order, and in `FMSProfilingParser`, which zips them with the column labels.
- New metrics are named `<base>_<stat>_<dimension>`, as the module docstring of `metrics.py` lays out; the
  predefined ones keep their short names.

### Runs

- **A `run` dimension appears only where it is needed.** `parse_profiling_data` gives a log one only when more than
  one run produced it, and selection and plotting refuse it (`_RUN_DIM_ERROR`) until `select_best_run` or
  `aggregate_runs` has removed it.
- **`select_best_run` keeps a run that took place; `aggregate_runs` does not.** The first picks one run by one metric
  and keeps it in every log; the second reduces each region and metric on its own, so its minimum can mix runs.
  They answer different questions, so don't substitute one for the other.

### Experiments

- **The experiment name is the key across sessions.** It is the key of `experiments`, the experiment's branch and
  directory under `work_dir`, Payu's `experiment`, and the name of its tarball. `Application.experiment_name` must
  therefore be distinct for every layout and the same every time for the same one, and changing how a model names
  its layouts orphans every experiment already on disk.
- **No manager state is stored.** A new manager finds the tarballs in `archive_dir` again, as ARCHIVED; every other
  status is read afresh from what the engine recorded, by `update_statuses`, which archiving, parsing and running
  all call first. ARCHIVED is terminal, since reading a status out of a tarball would mean extracting it.
- **`parse_status` never raises.** When it cannot tell, it logs why at DEBUG and returns `None`, which keeps the
  status the experiment has.
- **Generation registers nothing unless all of it succeeds.** `generate_scaling_experiments` plans without touching
  the disk, then calls `_create_experiments` once with every plan. If that raises, no new experiment is registered,
  because one registered with nothing on disk would be run and never generated again. `_create_experiments` returns
  only the new experiments, not those being regenerated (`ExperimentPlan.regenerating`), which are held already. A
  plan's changes are computed afresh on every call, never replayed.
- **Regenerating leaves a FAILED experiment FAILED**: the failure is the run's record, and
  `run_experiments(retry_failed=True)` is what clears it.
- **An archive holds only what can be extracted again**, since `directory()` extracts it with `filter="data"`,
  which refuses absolute links and links leading out of the archive. A symlink to inside the experiment is kept, as
  a relative link; one to outside it is followed with `follow_symlinks=True`, and otherwise left out and logged at
  INFO. `exclude_dirs` and `exclude_files` are matched against the paths in the archive, never the absolute ones.

### Core counts

- **`ncpus` is the number of cores the job was charged for, in whole nodes**, not the number the layout puts to
  work (`ProfilingManager.parse_ncpus`). Selection and the scaling plots group experiments by it.
- **The Payu models ask for the whole budget, idle cores included**: the submodel models write `ncpureq`
  (`SubmodelsConfiguration.config_changes`), OM3 `ncpus`. Layouts of one size are therefore jobs of one size.
- `PayuManager` reads the request PBS recorded in Payu's job files, and otherwise works it out from `config.yaml` the
  way Payu does, with Payu's defaults copied into `_PAYU_DEFAULT_NODE_SIZE` and `_PAYU_DEFAULT_NCPUS` because Payu
  exposes them nowhere. They have to follow Payu if it changes them.
- `CylcRoseManager` reads the `#PBS -l ncpus=` requests in the job scripts Cylc submitted, and takes the smallest
  that covers the layout; when no job asked for that many, it reports the layout's own count.

### Applications

- **An application is a frozen, keyword-only dataclass that validates on construction**, in `__post_init__`.
  `name` is required and an empty one is rejected; every other field has a default. A variant is made with
  `dataclasses.replace`, which validates it again.
- **A Payu model is put together from components** (`PayuComponent`): a component states what its own code
  decomposes, writes and logs, once, in `models/components.py`, and a configuration arranges components, deriving
  them from its fields (`ESM16Configuration.submodels`). `SubmodelsConfiguration` writes and reads the layout of
  every model Payu couples as submodels, so a model adds no code for that. A component's `name` and `token` are
  user-facing: the first keys allocation strategies, the second appears in every experiment name.
- **Presets are module-level values with a docstring on the line after**, which is what puts them in the API
  reference. A model ships one control source, beside its preset rather than on it, and not one per release of its
  configurations repository.
- **A model's component tree holds requirements only**, what is true of every layout of it; a study's preferences
  belong in its allocation strategy (the docstring of `ESM16Configuration.parallel_component`).
- **`CICEPartitioning` must stay hashable**: `_choose_blocks` is cached on it with `functools.cache`.
- **`config_changes` returns what experiment-generator merges into the control, in its format**: a list is wrapped
  in another list, and `"PRESERVE"` holds a position, because the generator merges lists by position and truncates
  them. [Adding an application](documentation/docs/pages/developer-guide/adding-an-application.md) has the details.
  The keys of the generator's and the runner's configurations, `model_type`, `submodels` and the keys of Payu's
  `config.yaml` are other libraries' words: don't rename them.

### Outside the package

- **The only commands run are `payu sweep --hard`, `rose suite-run` and `qstat -x -f -F json <job>`**, through
  `subprocess.run`, and nothing calls git or the Cylc CLI. PBS's view of a job is the `qstat -f -F json` record Payu
  keeps in its job files; PBS itself is asked only about a latest run whose job file records no verdict, to tell a
  run still going from one PBS killed (`PayuManager._job_is_over`). That call has a timeout, and any failure to make
  it reads as the run still going.
- **A control source never fetches anything**; it records where a control is. Fetching is left to the runner, which
  keeps credential prompts out of this package (the module docstring of `control.py`).
- **Logging** is `logging.getLogger(__name__)` in each module that logs, with no handlers and no `warnings.warn`: a
  warning to the user is a `logger.warning`. Tests assert on `caplog.text`, so rewording a message can fail one.

## Tests

`tests/test_<module>.py` per source module, flat (no `tests/models/`), with these exceptions:

| Test file | Covers |
|---|---|
| `test_cice5_profiling.py` | `cice5_parser.py` |
| `test_esmf_summary_parser.py` | `esmf_parser.py` |
| `test_cylc_dbreader.py` | `CylcDBReader`, together with `TestFailedTasksAreSkipped` in `test_cylc_parser.py` |
| `test_cice.py` | `models/cice.py`, the block partitioning, not the CICE parser |
| `test_components.py` | `models/components.py`, the components the Payu models are put together from |
| `test_esm16.py`, `test_om3.py` | their model, and `PayuManager` driving it end to end |
| `test_init.py` | `__init__.py`: that `__all__` lists exactly what the package imports |

Coverage is **100% of lines** across `access.profiling`, and a change keeps it there, error paths included. Nothing
enforces it (there is no `fail-under`), so read the report for the files you changed:

```shell
python -m pytest -o addopts="" --cov=access.profiling --cov-report=term-missing
```

Coverage says a line ran, not that anything would notice it changing. To check that a test binds, break the code it
covers and confirm that *that* test fails, then clear the bytecode
(`find . -name __pycache__ -type d -not -path "./.git/*" | xargs rm -rf`): a same-length edit reverted within the
same second leaves the mutant's `.pyc` loaded.

- **`unittest.mock`, never `monkeypatch`**: `from unittest import mock`, mostly `mock.patch` as a decorator.
- **Patch where the name is looked up.** Each module binds its own reference with `from ... import`, so it is
  `mock.patch("access.profiling.payu_manager.ExperimentGenerator")`; patching where the class is defined misses it.
- **Fake the engine, keep the rest real.** Faked: experiment-generator and experiment-runner (their classes in
  `payu_manager`), `subprocess.run`, and the plotting functions `manager` calls. Real: the layout search, files
  under `tmp_path`, including Payu's job files (`write_job_file` and `pbs_job_info` in `test_payu_manager.py`), Cylc
  suite databases written with `sqlite3`, and tarballs.
- A stand-in is `mock.Mock(spec=...)`, as `fake_parser` in `conftest.py` is. `MagicMock(autospec=True)` only sets
  an attribute named `autospec`.
- **Sample logs are inline**, as triple-quoted strings in module-scoped fixtures; there is no `tests/data/`. Module
  scope is for immutable data and parsers, function scope for anything mutable, such as a manager or a dataset.
- **`conftest.py` has one fixture**, `close_figures`, which closes the figures every test drew. The rest are
  helpers, imported with `from conftest import ...`: those that narrow an optional result with an assertion message
  rather than an `AttributeError` (`component_of`, `layout_of`, `decomposition_of`, `grid_of`, `legend_labels`), and
  those more than one module needs (`fake_parser`, and `select_layouts` for a Payu application). The import works
  because `tests/` has no `__init__.py`: don't add one.
- Newer files group tests in `class Test<Behaviour>:` blocks with no base class and no setup, the class docstring
  stating the behaviour. `pytest.raises` gets a `match=`. A test's docstring says what is checked and, where it is
  not obvious, why. Tests carry no type annotations.

## Commands

```shell
pip install -e ".[devel,test]"   # in a virtual environment
pre-commit install
python -m pytest                 # coverage is on by default, through the pyproject addopts
python -m pytest tests/test_esm16.py -q --no-cov   # a fast run of one file
ruff format .
ruff check .
pre-commit run --all-files       # what CI's formatting job runs
python documentation/scripts/generate_figures.py   # after changing the plotting functions
```

`python -m pytest` writes `.coverage`, `coverage.xml` and `htmlcov/` into the repository root; all three are
ignored. The documentation is built in an environment of its own, the way CI builds it:

```shell
pip install -r documentation/requirements.txt
READTHEDOCS_CANONICAL_URL=https://access-profiling.readthedocs.io/latest/ \
    mkdocs build --strict -f documentation/mkdocs.yml
```

The variable stands in for the one Read the Docs sets, without which the build refuses to run. The date at the
foot of each page comes from git, so a page that has not been committed yet makes the strict build warn.

CI (`.github/workflows/ci.yml`) runs pre-commit, a PyPA build, a conda build, pytest on 3.11, 3.12 and 3.13 with the
coverage uploaded to Codecov, and the strict documentation build. Releases are tag-driven (`v*`, to PyPI and the
accessnri conda channel); there is nothing to run locally for one.

## Conventions

- Apache-2.0 header on every source and test file: the two-line
  `# Copyright 2025 ACCESS-NRI and contributors...` / `# SPDX-License-Identifier: Apache-2.0` block. `__init__.py`
  of the package is the one without it.
- ruff, line length 120, rules `E,F,B,C4,SIM,PTH,UP,I,N,C901`. `PTH` keeps the code on `pathlib`. `C901` is
  enforced: split the function rather than silencing it.
- Parameters and return values are annotated, with `X | None` and built-in generics and without
  `from __future__ import annotations`. Tests are the exception.
- Data types are `@dataclass(frozen=True)`; `__post_init__` validates, and normalises with `object.__setattr__`.
- **Docstrings are Google style with Markdown markup**, as
  [Contributing](documentation/docs/pages/developer-guide/contributing.md) sets out: a blank line before each
  section, `name (type): description`, single backticks, and fenced blocks for examples and for samples of the logs
  a parser reads. mkdocstrings renders them and CI builds strictly, so a docstring it cannot read fails the build.
  The reason for a rule is written once, in the module docstring or in a comment beside the code it explains.
- What a subclass must provide is declared abstract in its base class, `_`-prefixed members included
  (`_create_experiments`, `_delete_experiments`), except `_metrics`, which a parser sets as a class attribute or in
  `__init__`. `_`-prefixed names are left out of the API reference, so the contracts table of the developer guide
  lists them all.
- `documentation/` comes from the ACCESS-NRI template,
  [documentation-infra](https://github.com/ACCESS-NRI/documentation-infra), and is kept so that it can be updated
  from there: `hooks/`, `overrides/`, `docs/css/`, `docs/js/`, `docs/fontello/`, `docs/assets/logos/`,
  `requirements.txt` and `.readthedocs.yaml` stay as copied, and `mkdocs.yml` and `abbreviations.md` are changed
  only where the template marks them for it.
- Credit the agent in commit messages, with a `Co-Authored-By:` trailer.

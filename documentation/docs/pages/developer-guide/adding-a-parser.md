# Adding a parser

A log format the package does not read is added as a new parser. In this tutorial we write one for a simple table
of timings, then look at the other shapes a parser can return and at how an application uses it. The parsers in
`src/access/profiling/*_parser.py` are complete examples.

## The parser class

A parser subclasses [`ProfilingParser`][access.profiling.parser.ProfilingParser]. It states the metrics it reports
in `_metrics`, and implements [`parse`][access.profiling.parser.ProfilingParser.parse], which reads a file and
returns the values of every metric for every region. This is the log ours reads:

```text
region     calls        min        max       mean
total          1    3601.20    3603.75    3602.48
timestep    1440    3410.62    3425.07    3417.93
output        24      98.14     120.33     104.71
```

The columns are each region's number of calls, and the minimum, maximum and mean across processes of the time
spent in it, which is what the predefined `count`, `tmin`, `tmax` and `tavg` metrics are. So the parser reports
those four:

```python
import re
from pathlib import Path

from access.profiling.metrics import count, tavg, tmax, tmin
from access.profiling.parser import ProfilingData, ProfilingParser

LINE = re.compile(r"(?P<region>\S+)\s+(?P<calls>\d+)\s+(?P<min>[\d.]+)\s+(?P<max>[\d.]+)\s+(?P<mean>[\d.]+)")


class TimingTableParser(ProfilingParser):
    """Parser for a table of timings with a header line, then one line per region: its name, its number of calls,
    and the minimum, maximum and mean across processes of the time spent in it."""

    def __init__(self):
        super().__init__()
        self._metrics = [count, tmin, tmax, tavg]

    def parse(self, file_path) -> ProfilingData:
        stats: ProfilingData = {"region": [], count: [], tmin: [], tmax: [], tavg: []}
        _header, *lines = Path(file_path).read_text().splitlines()
        for line in lines:
            match = LINE.fullmatch(line.strip())
            if match is None:
                raise ValueError(f"Unexpected line in {file_path}: {line!r}")
            stats["region"].append(match["region"])
            stats[count].append(int(match["calls"]))
            stats[tmin].append(float(match["min"]))
            stats[tmax].append(float(match["max"]))
            stats[tavg].append(float(match["mean"]))
        if not stats["region"]:
            raise ValueError(f"No timings found in {file_path}")
        return stats
```

The returned dictionary holds the region names under `"region"`, and the values of each metric, in the same order,
under the metric itself. Every metric in `_metrics` must be there, and the values are numbers in the metric's
units: the units are attached when the values become a dataset.

Raise a `ValueError` when the file is not in the expected format, and do not return what could be read: a partial
result would be plotted as if it were complete. A log that a run may legitimately not write, or write differently,
is declared optional by the application instead.

[`ProfilingLog`][access.profiling.experiment.ProfilingLog] turns the result into the dataset the manager stores.
Try it on the table above, saved as `timings.txt`:

```python
from access.profiling.experiment import ProfilingLog

print(ProfilingLog(Path("timings.txt"), TimingTableParser()).parse())
```

The output should be:

```text
<xarray.Dataset> Size: 192B
Dimensions:       (region: 3)
Coordinates:
  * region        (region) <U8 96B 'total' 'timestep' 'output'
Data variables:
    count         (region) int64 24B [] 1 1440 24
    minimum time  (region) float64 24B [s] 3.601e+03 3.411e+03 98.14
    maximum time  (region) float64 24B [s] 3.604e+03 3.425e+03 120.3
    average time  (region) float64 24B [s] 3.602e+03 3.418e+03 104.7
```

## New metrics

A quantity the predefined metrics do not cover gets a [`ProfilingMetric`][access.profiling.metrics.ProfilingMetric]
of its own, defined next to the parser that reports it, as `grain` is in `fms_parser.py`. For example, the maximum
across PEs of the mean time per call:

```python
from pint import Unit

from access.profiling.metrics import ProfilingMetric

tavg_max_pe = ProfilingMetric("tavg_max_pe", Unit("second"), "Maximum across PEs of the mean time per call")
```

The name follows the convention of `access.profiling.metrics`, `<base>_<stat>_<dimension>`, which says what the
statistic is taken over. The description is the title of the bar charts that plot the metric.

## Other shapes of data

A parser can return two other shapes, which `ProfilingLog` handles as well:

- **Hierarchical**: nested dictionaries following the call tree of the regions, with no `"region"` key. A string
  key is a child region, and a metric key one of the region's values. The tree is flattened into the usual list
  of regions, parents before their children. `ESMFSummaryProfilingParser(hierarchical=True)` returns this shape.
- **Per PE**: the flat shape plus a `"pe"` key listing the PEs, with the value of every metric given for each PE
  of each region. The dataset then has a `pe` dimension, which
  [`aggregate_pe_data`][access.profiling.parser.aggregate_pe_data] reduces to statistics across the PEs, keyed
  by the metrics [`pe_statistic`][access.profiling.parser.pe_statistic] returns.

## Using the parser

An application declares the logs its components write, each with its parser, as a
[`LogSpec`][access.profiling.application.LogSpec]. For example, to read our table from the atmosphere's output
directory:

```python
from access.profiling.application import LogSpec, log_at

LogSpec("timings", log_at("atmosphere/timings.txt"), TimingTableParser(), component="atm")
```

[Adding an application](adding-an-application.md#the-component) explains the fields. A parser for the task logs of a
Rose suite goes in the `parsers` of its `RoseSuiteConfiguration` instead.

## Testing it

The parser tests in `tests/` write a sample of the log to a temporary file and check what the parser makes of it,
including the errors it raises on files it cannot read. Here are two tests for ours:

```python
import pytest

from access.profiling.metrics import tmax


def test_timing_table(tmp_path):
    log = tmp_path / "timings.txt"
    log.write_text("region calls min max mean\ntotal 1 3601.20 3603.75 3602.48\n")

    stats = TimingTableParser().parse(log)

    assert stats["region"] == ["total"]
    assert stats[tmax] == [3603.75]


def test_timing_table_with_an_unexpected_line(tmp_path):
    log = tmp_path / "timings.txt"
    log.write_text("region calls min max mean\ntotal one 1 2 3\n")

    with pytest.raises(ValueError, match="Unexpected line"):
        TimingTableParser().parse(log)
```

If you add the parser to the package, give it a module docstring with a sample of the format it reads. For the
formats of the ACCESS models, add an entry in the API reference and in the table of
[Log formats](../access/log-formats.md): `documentation/docs/pages/api/log-formats.md` and
`documentation/docs/pages/access/log-formats.md`.

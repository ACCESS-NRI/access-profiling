# Adding a parser

Does your application write its timings in a format that the package cannot read? Then you need a new parser. In
this tutorial we write one for a simple table of timings. We then look at the other kinds of data a parser can
return, and at how an application uses a parser. For complete examples, see the parsers in
`src/access/profiling/*_parser.py`.

## The parser class

A parser is a subclass of [`ProfilingParser`][access.profiling.parser.ProfilingParser]. It has to do two things:
list its metrics in `_metrics`, and implement [`parse`][access.profiling.parser.ProfilingParser.parse], which
reads a file and returns the values of each metric for each region. This is the log that our parser reads:

```text
region     calls        min        max       mean
total          1    3601.20    3603.75    3602.48
timestep    1440    3410.62    3425.07    3417.93
output        24      98.14     120.33     104.71
```

For each region, the log gives the number of calls, and the minimum, the maximum and the mean over the processes
of the time spent in it. The package already has metrics for these: `count`, `tmin`, `tmax` and `tavg`. So our
parser uses those four:

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

The method returns a dictionary. The names of the regions are under `"region"`, and the values of each metric are
under the metric itself, in the same order as the regions. All the metrics of `_metrics` must be there. The values
are plain numbers, in the units of the metric. The units are added later, when the values are put in a dataset.

If the file is not in the expected format, raise a `ValueError`. Do not return the part that you could read,
because it would then be plotted as if it was complete. If a run may not write the log at all, or may write it
differently, this is dealt with in the application, which can mark the log as optional.

To get the same dataset that the manager would store, use a
[`ProfilingLog`][access.profiling.experiment.ProfilingLog]. Try it on the table above, saved as `timings.txt` in
the current directory. If you saved it somewhere else, adapt the path:

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

What if your log has something that the predefined metrics do not cover? In that case, create a new
[`ProfilingMetric`][access.profiling.metrics.ProfilingMetric] in the same module as the parser, as is done for
`grain` in `fms_parser.py`. For example, for the maximum across PEs of the mean time per call:

```python
from pint import Unit

from access.profiling.metrics import ProfilingMetric

tavg_max_pe = ProfilingMetric("tavg_max_pe", Unit("second"), "Maximum across PEs of the mean time per call")
```

The name follows the convention of `access.profiling.metrics`, which is `<base>_<stat>_<dimension>`. This makes
it clear what the statistic is taken over. The description is used as the title of the bar charts of the metric.

## Other shapes of data

The dictionary above is the simplest kind of data. A parser can also return two other kinds, and `ProfilingLog`
deals with both:

- **Hierarchical**: nested dictionaries following the call tree of the regions, with no `"region"` key. A string
  key is a child region, and a metric key one of the region's values. The tree is flattened into the usual list
  of regions, parents before their children. `ESMFSummaryProfilingParser(hierarchical=True)` returns this shape.
- **Per PE**: the flat shape plus a `"pe"` key listing the PEs, with the value of every metric given for each PE
  of each region. The dataset then has a `pe` dimension, which
  [`aggregate_pe_data`][access.profiling.parser.aggregate_pe_data] reduces to statistics across the PEs, keyed
  by the metrics [`pe_statistic`][access.profiling.parser.pe_statistic] returns.

## Using the parser

To use the parser, an application has to know about it. Each application lists its logs and their parsers, as
[`LogSpec`][access.profiling.application.LogSpec]s. For example, to read our table from the output directory of
the atmosphere:

```python
from access.profiling.application import LogSpec, log_at

LogSpec("timings", log_at("atmosphere/timings.txt"), TimingTableParser(), component="atm")
```

The path is relative to the output directory of a run, so adapt it to where your application writes the log. The
fields are explained in [Adding an application](adding-an-application.md#the-component). For a Rose suite, the
parsers of the task logs go in the `parsers` of its `RoseSuiteConfiguration` instead.

## Testing it

Do not forget the tests! The tests of the parsers in `tests/` all work in the same way. They write a sample of
the log to a temporary file, and check what the parser returns. They also check the errors that the parser raises
when it cannot read a file. Here are two tests for ours:

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

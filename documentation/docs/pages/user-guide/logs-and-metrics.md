# Logs and metrics

The runs of an application write profiling logs: the timers of its components, and the records the workflow engine
keeps of every job. The application declares the logs its runs write, each with the parser that reads it, and a
parser reports a few metrics for every region its log times. The metrics are drawn from one set, described below,
and [Log formats](../access/log-formats.md) lists the parsers the package provides for the logs of the ACCESS
models.

## Logs and parsers

Every log an application declares is looked for in the output of each run. Its parser reads it into a dataset
holding the metrics of every region the log times, named as the log names them;
[Concepts](index.md#profiling-data) shows such a dataset. A log may be declared optional, when a run can
legitimately leave it out or write it differently.
[Adding an application](../developer-guide/adding-an-application.md#the-component) shows how an application declares
its logs, and [Adding a parser](../developer-guide/adding-a-parser.md) how a new log format is read.

## Metrics

The metrics are [`ProfilingMetric`][access.profiling.metrics.ProfilingMetric] objects. The predefined ones are
constants in `access.profiling.metrics`:

| Constant | Name | Units | Meaning |
|---|---|---|---|
| `count` | count | | Number of times the region was entered |
| `tmin` | minimum time | s | Minimum across PEs of the total time spent in the region |
| `tmax` | maximum time | s | Maximum across PEs of the total time spent in the region, or the one elapsed time of logs that record no more |
| `tavg` | average time | s | Mean across PEs of the total time spent in the region |
| `tmed` | median time | s | Median across PEs of the total time spent in the region |
| `tstd` | time std | s | Standard deviation across PEs of the total time spent in the region |
| `tfrac` | time fraction | % | Mean time in the region as a percentage of the total runtime |
| `pemin` | minimum PE | | PE on which the minimum time was recorded |
| `pemax` | maximum PE | | PE on which the maximum time was recorded |

The time metrics are statistics of one quantity. Each PE adds up the time it spends in a region over all its calls
to it, and the statistics summarise those totals across the PEs that ran the region. `tmax` is therefore the time
of the slowest PE, and `tmax - tavg` is how much longer that PE took than an average one. Note that logs which
record one elapsed time per region, and no distribution over the PEs, store it as `tmax`.

A parser can also report quantities of its own, as metrics defined next to it. Some of the
[log formats of the ACCESS models](../access/log-formats.md#metrics-of-their-own) do.

## Things to watch for

- When a log names two regions the same way, the second is renamed with a `_2` suffix, the third with `_3`, and so
  on.
- An error parsing a log stops `parse_profiling_data`, unless the log is optional. An error in an optional log is
  logged at INFO, and the log is skipped.

## Parsing a log yourself

A [`ProfilingLog`][access.profiling.experiment.ProfilingLog] pairs a file with its parser, and parses it into the
same dataset the manager stores. For example, to read the standard output of MOM5 with the parser of its FMS
timers:

```python
from pathlib import Path

from access.profiling import FMSProfilingParser, ProfilingLog

ds = ProfilingLog(Path("mom5.out"), FMSProfilingParser(has_hits=False)).parse()
```

You can see the result for the [MOM5 sample](../access/log-formats.md#parsers) of the log formats in
[Concepts](index.md#profiling-data). If you call a parser's own `parse` method instead, it returns the values it
read as a plain dictionary, keyed by `"region"` and by metric.

A parser that reports each PE separately gives its dataset a `pe` dimension.
[`aggregate_pe_data`][access.profiling.parser.aggregate_pe_data] reduces it to statistics across the PEs, each
keyed by the metric [`pe_statistic`][access.profiling.parser.pe_statistic] returns for it. The maximum across PEs
of `tavg`, for example, is `aggregate_pe_data(ds)[pe_statistic(tavg, "max")]`.

# Logs and metrics

When an application runs, it writes profiling logs. These are the timers of its components, and the records that
the workflow engine keeps of every job. Each log is read by a parser, which returns a few numbers, called metrics,
for every region that the log times. Here you will find how the logs are read and what the metrics mean. The
parsers for the logs of the ACCESS models are listed in [Log formats](../access/log-formats.md).

## Logs and parsers

Each application knows which logs its runs write, and which parser reads each of them. The manager looks for
these logs in the output of every run. Each log is then read into a dataset with the metrics of every region, and
the regions have the same names as in the log. You can see an example of such a dataset in
[Concepts](index.md#profiling-data). Some logs are optional, which means that it is fine if a run does not write
them. If you want to know how an application lists its logs, see
[Adding an application](../developer-guide/adding-an-application.md#the-component), and to read a new log format,
see [Adding a parser](../developer-guide/adding-a-parser.md).

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

All the time metrics are computed in the same way. First, each PE adds up all the time it spends in a region.
Then the minimum, the maximum, the mean and so on are taken over all the PEs that ran the region. This means that
`tmax` is the time of the slowest PE, and `tmax - tavg` is how much longer that PE took than an average one. Note
that some logs only give one time per region. In that case the time is stored as `tmax`.

A parser can also have metrics of its own, for quantities that the predefined metrics do not cover. Some of the
[log formats of the ACCESS models](../access/log-formats.md#metrics-of-their-own) do.

## Things to watch for

- When a log names two regions the same way, the second is renamed with a `_2` suffix, the third with `_3`, and so
  on.
- If a log cannot be read, `parse_profiling_data` stops with an error. The exception is an optional log, which is
  simply skipped, with an INFO message.

## Parsing a log yourself

You do not need a manager to read a log. A [`ProfilingLog`][access.profiling.experiment.ProfilingLog] takes a
file and a parser, and gives you the same dataset the manager would. For example, to read the standard output of
MOM5 with the parser of its FMS timers:

```python
from pathlib import Path

from access.profiling import FMSProfilingParser, ProfilingLog

ds = ProfilingLog(Path("mom5.out"), FMSProfilingParser(has_hits=False)).parse()
```

Here `mom5.out` stands for the log, so adapt the path to where your own log is. You can see the result for the
[MOM5 sample](../access/log-formats.md#parsers) of the log formats in [Concepts](index.md#profiling-data). If you
call a parser's own `parse` method instead, it returns the values it read as a plain dictionary, keyed by
`"region"` and by metric.

Some parsers may return the values of every PE separately. Their datasets then have a `pe` dimension. You can
use [`aggregate_pe_data`][access.profiling.parser.aggregate_pe_data] to compute statistics over the PEs. Each
statistic is accessed with the metric that [`pe_statistic`][access.profiling.parser.pe_statistic] returns for it.
The maximum across PEs of `tavg`, for example, is `aggregate_pe_data(ds)[pe_statistic(tavg, "max")]`.

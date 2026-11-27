# Results

The results are what [`parse_profiling_data`][access.profiling.manager.ProfilingManager.parse_profiling_data]
returns: the parsed profiling data of a study, with the labels, the layout and the number of CPUs of each
experiment. They select the experiments, reduce their runs, and make the tables and the plots. They never change:
a selection or a reduction returns new results.

::: access.profiling.results

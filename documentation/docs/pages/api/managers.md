# Managers

A manager runs a profiling study: it generates the experiments or registers ones that already exist, submits
them, follows their status, archives them, and parses, selects, plots and compares what they produced.
[`ProfilingManager`][access.profiling.manager.ProfilingManager] does the work common to every workflow engine, and
each subclass adds what one engine does. The managers for the workflow engines of the ACCESS models are documented
under [Payu](payu.md) and [Cylc and Rose](cylc-rose.md).

::: access.profiling.manager

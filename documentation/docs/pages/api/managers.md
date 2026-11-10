# Managers

A manager runs a profiling study. It creates the experiments, or registers existing ones, submits them and follows their
status. It can also archive them, read their logs, and select, plot and compare the results.
[`ProfilingManager`][access.profiling.manager.ProfilingManager] does everything that is the same for all workflow
engines, and each subclass adds what is specific to one engine. The managers for the workflow engines of the ACCESS
models are documented under [Payu](payu.md) and [Cylc and Rose](cylc-rose.md).

::: access.profiling.manager

# Log formats

These are the parsers the package provides for the logs the ACCESS models write, and for the records of the
workflow engines that run them. [ACCESS models](index.md) says which logs each model writes, and
[Logs and metrics](../user-guide/logs-and-metrics.md) what the metrics mean.

## Parsers

| Parser | Reads | Metrics | Regions, for example |
|---|---|---|---|
| [`FMSProfilingParser`][access.profiling.fms_parser.FMSProfilingParser] | The `mpp_clock` summary FMS writes into the standard output of MOM5 and MOM6 | `count` (MOM6 only), `tmin`, `tmax`, `tavg`, `tstd`, `tfrac`, `grain`, `pelist_min`, `pelist_max` | `Total runtime`, `Ocean dynamics`, `oasis_recv` |
| [`ESMFSummaryProfilingParser`][access.profiling.esmf_parser.ESMFSummaryProfilingParser] | ESMF's profiling summary, `ESMF_Profile.summary` | `pets`, `pes`, `count`, `tavg`, `tmin`, `pemin`, `tmax`, `pemax` | `[ESMF]`, `[OCN] RunPhase1`, `[ICE] RunPhase1` |
| [`CICE5ProfilingParser`][access.profiling.cice5_parser.CICE5ProfilingParser] | The timers CICE5 and CICE6 write at the end of a run | `tmin`, `tmax`, `tavg` | `Total`, `TimeLoop` |
| [`UMProfilingParser`][access.profiling.um_parser.UMProfilingParser] | The UM's inclusive timer summary, in the standard output of rank 0 | `tavg`, `tmed`, `tstd`, `tmax`, `pemax`, `tmin`, `pemin` | `AS3 Atmos_Phys2` (UM7), `U_MODEL_4A` (UM13) |
| [`UMTotalRuntimeParser`][access.profiling.um_parser.UMTotalRuntimeParser] | The UM's maximum elapsed wallclock time, in the same file | `tmax` | `um_total_walltime` |
| [`PayuJSONProfilingParser`][access.profiling.payujson_parser.PayuJSONProfilingParser] | The timings in Payu's job file of a run | `tmax` | `payu_model_run_duration_seconds`, `payu_total_duration_seconds` |
| [`CylcProfilingParser`][access.profiling.cylc_parser.CylcProfilingParser] | A Cylc suite log | `tmax` | `pipeline_elapsed_time` |
| [`CylcDBReader`][access.profiling.cylc_parser.CylcDBReader] | The task jobs of a Cylc suite database | `tmax` | `<task>_cycle<cycle>` |

The regions are named as the log names them. Payu's job file records the time it spent setting up, running the
model, archiving and in total, as `payu_setup_duration_seconds`, `payu_model_run_duration_seconds`,
`payu_archive_duration_seconds` and `payu_total_duration_seconds`, and `payu_run_duration_seconds` for the whole run
command. Note that `CICE5ProfilingParser` reads the statistics CICE computes over its processes, and skips the ones
it computes over blocks.

Each format looks like this:

??? example "FMS, in the standard output of MOM5"

    ```text
    Tabulating mpp_clock statistics across     49 PEs...

                                             tmin          tmax          tavg          tstd  tfrac grain pemin pemax
    Total runtime                    16282.797785  16282.797792  16282.797789      0.000001  1.000     0     0    48
    Ocean                            15969.542784  16000.704550  15986.765795      8.643639  0.982     1     0    48
    (Ocean initialization)               4.288529      4.296586      4.291991      0.001470  0.000    11     0    48
    oasis_recv                         168.797136    171.648384    170.460762      0.650894  0.010    31     0    48
     MPP_STACK high water mark=          0
    ```

    Note that MOM6 writes a `hits` column before `tmin`, which is why the MOM6 logs are read with `has_hits=True`.

??? example "ESMF summary"

    ```text
    Region                         PETs   PEs    Count    Mean (s)    Min (s)     Min PET Max (s)     Max PET
      [ESMF]                       1664   1664   1        2558.5684   2555.1450   279     2559.5801   817
        [ICE] RunPhase1            364    364    960      155.8202    154.7637    94      160.2443    0
          cice_run_total           364    364    960      155.4648    154.3687    94      159.8980    0
        [MED-TO-OCN] RunPhase1     1664   1664   960      16.7498     0.4588      256     23.2875     1023
    ```

??? example "CICE timers"

    ```text
    Timer   1:     Total   16197.47 seconds
      Timer stats (node): min =    16197.42 seconds
                          max =    16197.47 seconds
                          mean=    16197.44 seconds
      Timer stats(block): min =        0.00 seconds
                          max =        0.00 seconds
                          mean=        0.00 seconds
    ```

??? example "UM timer summary"

    ```text
    MPP : Inclusive timer summary

    WALLCLOCK  TIMES
    N  ROUTINE                                MEAN       MEDIAN        SD   % of mean          MAX  (PE)          MIN  (PE)
    01 U_MODEL_4A                          1314.55      1314.55      0.06       0.00%      1315.88 (  0)      1314.55 (433)
    02 Atm_Step_4A (AS)                    1272.16      1273.09      4.60       0.36%      1279.04 (240)      1257.69 ( 27)

    CPU TIMES (sorted by wallclock times)
    ```

    The UM's total runtime comes from the end of the same file:

    ```text
     Maximum Elapsed Wallclock Time:    3944.07699399998
    ```

## Metrics of their own

Two of the formats report quantities none of the others do, as metrics defined next to their parsers: `grain`,
`pelist_min` and `pelist_max` in `access.profiling.fms_parser`, and `pets` and `pes` in
`access.profiling.esmf_parser`.

| Constant | Name | Units | Meaning |
|---|---|---|---|
| `grain` | grain | | Level of detail of an FMS clock: 1 for a whole component, and larger for finer clocks |
| `pelist_min` | lowest PE | | Lowest PE of the list of PEs an FMS clock is kept on |
| `pelist_max` | highest PE | | Highest PE of the list of PEs an FMS clock is kept on |
| `pets` | PETs | | Number of ESMF persistent execution threads that ran the region |
| `pes` | PEs | | Number of ESMF processing elements that ran the region |

The timers of FMS, ESMF, CICE and the UM report their times as statistics across PEs of the time spent in each
region, as [Logs and metrics](../user-guide/logs-and-metrics.md#metrics) describes. Payu's job timings and the Cylc
logs record a single time per region, as `tmax`. All of them report one value per region, and none one per PE.

## Things to watch for

- FMS truncates clock names to 32 characters, so a region may have to be named as it is truncated, such as
  `Ocean grid generation and remapp`.
- `FMSProfilingParser` takes every line of the table for a clock, and raises an error on a line it cannot read
  instead of leaving a clock out.
- By default, `ESMFSummaryProfilingParser` merges the regions of the same name found at several places of the call
  tree into one region. Their counts are added, their mean times are averaged weighted by their counts, and the
  smallest minimum and the largest maximum are kept. With `hierarchical=True` it keeps the call tree instead.
- The model logs of ACCESS-OM3 and the UM logs of the Rose suites are optional, so an error in one of them is
  logged at INFO, and the log is skipped. A log that a Payu run did not write at all is simply left out.

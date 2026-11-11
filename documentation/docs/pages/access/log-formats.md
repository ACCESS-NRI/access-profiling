# Log formats

Here you will find the parsers for the logs of the ACCESS models, and for the records of their workflow engines.
To see which logs each model writes, go to [ACCESS models](index.md). The metrics are explained in
[Logs and metrics](../user-guide/logs-and-metrics.md).

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

The regions have the same names as in the logs. The job file of Payu gives the time Payu spent setting up,
running the model, archiving and in total. These are the regions `payu_setup_duration_seconds`,
`payu_model_run_duration_seconds`, `payu_archive_duration_seconds` and `payu_total_duration_seconds`. There is
also `payu_run_duration_seconds`, for the whole run command. Note that CICE computes statistics over its processes
and over its blocks, and `CICE5ProfilingParser` only reads the ones over processes.

To give you an idea, this is what each format looks like:

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

Two of the formats have information that the other formats do not have, so their parsers define metrics of
their own. These are `grain`, `pelist_min` and `pelist_max` in `access.profiling.fms_parser`, and `pets` and `pes`
in `access.profiling.esmf_parser`.

| Constant | Name | Units | Meaning |
|---|---|---|---|
| `grain` | grain | | Level of detail of an FMS clock: 1 for a whole component, and larger for finer clocks |
| `pelist_min` | lowest PE | | Lowest PE of the list of PEs an FMS clock is kept on |
| `pelist_max` | highest PE | | Highest PE of the list of PEs an FMS clock is kept on |
| `pets` | PETs | | Number of ESMF persistent execution threads that ran the region |
| `pes` | PEs | | Number of ESMF processing elements that ran the region |

The timers of FMS, ESMF, CICE and the UM give statistics over the PEs of the time spent in each region, as
explained in [Logs and metrics](../user-guide/logs-and-metrics.md#metrics). Payu's job timings and the Cylc logs
only give one time per region, which is stored as `tmax`. None of these logs gives the values of each PE
separately.

## Things to watch for

- FMS truncates clock names to 32 characters, so a region may have to be named as it is truncated, such as
  `Ocean grid generation and remapp`.
- `FMSProfilingParser` expects every line of the table to be a clock. If it cannot read a line, it raises an
  error instead of silently leaving the clock out.
- In an ESMF summary, the same region can appear in several places of the call tree. By default,
  `ESMFSummaryProfilingParser` merges them into one region. The counts are added, the mean times are averaged
  using the counts as weights, and the smallest minimum and the largest maximum are kept. If you want to keep the
  call tree instead, use `hierarchical=True`.
- The model logs of ACCESS-OM3 and the UM logs of the Rose suites are optional. If one of them cannot be read, the
  log is skipped, with an INFO message. If a Payu run did not write a log at all, the log is simply left out.

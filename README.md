# FAD counters to GPS time

The FACT events are created by the event-builder program which assigns each event a time stamp based on the system time of the machine where the event-builder is running. This time stamp is only accurate in the ~1ms regime, e.g. due to ethernet network delays. However, for studies which need a high timing precision like pulsar studies, a more precise time stamp is needed.

## Install dependency:

    pip install git+https://github.com/fact-project/zfits --process-dependency-links

## Usage: make gps time fit foo

    from fact.credentials import create_factdb_engine
    import pandas as pd
    import fad_counters_to_gps_time as fad
    jobs = fad.gps_time_reconstruction.make_job_list.make_job_list(
        out_dir='/gpfs0/fact/processing/gps_time',
        run_info=pd.read_hdf('/gpfs0/fact/processing/fad_counters/fad/known_runs.h5'))
    fad.gps_time_reconstruction.isdc.qsub.qsub(jobs)

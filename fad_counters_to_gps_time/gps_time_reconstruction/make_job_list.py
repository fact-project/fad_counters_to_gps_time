from os.path import abspath
from os.path import join
from os.path import exists


OBSERVATION_RUN_KEY = 1


def make_job_list(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fad_counter_dir='/gpfs0/fact/processing/fad_counters/fad',
    tmp_dir_base_name='gps_time_reco_',
):
    """
    Returns a list of jobs, and output directory structure.
    A job is a dict with all paths needed to reconstruct the gps time from
    the fad counters.

    Parameters
    ----------

    out_dir             The path to the output directory where the gps times
                        are collected. The out_dir is created if not existing.

    run_info            A pandas DataFrame() of the FACT run-info-database which
                        is used as a reference for the runs to be processed.
                        (default None, download the latest run-info on the fly)

    only_a_fraction     A ratio between 0.0 and 1.0 to only process a
                        random fraction of the runs. Usefull for debugging over
                        long periodes of observations. (default 1.0)

    fad_counter_dir     The path to the FAD counters

    tmp_dir_base_name   The base name of the temporary directory on the qsub
                        worker nodes. (default 'gps_time_reco_JOB_ID_')
    """

    out_dir = abspath(out_dir)
    fad_counter_dir = abspath(fad_counter_dir)

    std_dir = join(out_dir, 'std')
    gps_time_dir = join(out_dir, 'gps_time')
    models_dir = join(out_dir, 'gps_time_models')

    jobs = observation_runs_in_run_info(run_info, only_a_fraction)
    for job in jobs:
        yyyymmnn_dir = '{y:04d}/{m:02d}/{n:02d}/'.format(
            y=night_id_2_yyyy(job['Night']),
            m=night_id_2_mm(job['Night']),
            n=night_id_2_nn(job['Night'])
        )
        base_name = '{bsn:08d}_{rrr:03d}'.format(
            bsn=job['Night'],
            rrr=job['Run']
        )
        job['input_file_path'] = join(
            fad_counter_dir,
            yyyymmnn_dir,
            base_name+'_fad.h5'
        )
        job['std_yyyy_mm_nn_dir'] = join(std_dir, yyyymmnn_dir)
        job['gps_time_yyyy_mm_nn_dir'] = join(gps_time_dir, yyyymmnn_dir)
        job['models_yyyy_mm_nn_dir'] = join(models_dir, yyyymmnn_dir)

        job['std_out_path'] = join(job['std_yyyy_mm_nn_dir'], base_name + '.o')
        job['std_err_path'] = join(job['std_yyyy_mm_nn_dir'], base_name + '.e')

        job['gps_time_path'] = join(job['gps_time_yyyy_mm_nn_dir'], base_name+'_gps_time.h5')
        job['models_path'] = join(job['models_yyyy_mm_nn_dir'], base_name+'_models.h5')

        job['is_file_existing'] = exists(job['input_file_path'])

    jobs = [job for job in jobs.values if job['is_file_existing']]

    return {
        'jobs': jobs,
        'directory_structure': {
            'out_dir': out_dir,
            'std_dir': std_dir,
            'gps_time_dir': gps_time_dir,
            'models_dir': models_dir,
        }
    }


def observation_runs_in_run_info(
    run_info,
    only_a_fraction=1.0
):
    run_info = run_info[run_info.fRunTypeKey == OBSERVATION_RUN_KEY].copy()
    run_info = run_info.sample(frac=only_a_fraction)

    jobs = []
    for run in run_info.itertuples():
        jobs.append({
            'Night': run.fNight,
            'Run': run.fRunID
            })
    return jobs


def night_id_2_yyyy(night):
    return night // 10000


def night_id_2_mm(night):
    return (night // 100) % 100


def night_id_2_nn(night):
    return night % 100

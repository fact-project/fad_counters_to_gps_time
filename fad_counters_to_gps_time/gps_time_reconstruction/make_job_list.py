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
        job['yyyy'] = night_id_2_yyyy(job['Night'])
        job['mm'] = night_id_2_mm(job['Night'])
        job['nn'] = night_id_2_nn(job['Night'])
        job['fad_counter_dir'] = fad_counter_dir
        job['yyyymmnn_dir'] = '{y:04d}/{m:02d}/{n:02d}/'.format(
            y=job['yyyy'],
            m=job['mm'],
            n=job['nn']
        )
        job['base_name'] = '{bsn:08d}_{rrr:03d}'.format(
            bsn=job['Night'],
            rrr=job['Run']
        )
        job['fad_counter_file_name'] = job['base_name']+'_fad.h5'
        job['fad_counter_file_path'] = join(
            job['fad_counter_dir'],
            job['yyyymmnn_dir'],
            job['fad_counter_file_name']
        )

    accesible_jobs = []
    for job in jobs:
        if exists(job['fad_counter_file_path']):
            accesible_jobs.append(job)
    jobs = accesible_jobs

    for job in jobs:
        job['std_dir'] = std_dir
        job['std_yyyy_mm_nn_dir'] = join(job['std_dir'], job['yyyymmnn_dir'])
        job['std_out_path'] = join(job['std_yyyy_mm_nn_dir'], job['base_name']+'.o')
        job['std_err_path'] = join(job['std_yyyy_mm_nn_dir'], job['base_name']+'.e')

        job['worker_tmp_dir_base_name'] = tmp_dir_base_name

        job['gps_time_dir'] = gps_time_dir
        job['gps_time_yyyy_mm_nn_dir'] = join(job['gps_time_dir'], job['yyyymmnn_dir'])
        job['gps_time_path'] = join(job['gps_time_yyyy_mm_nn_dir'], job['base_name']+'_gps_time.h5')

        job['models_dir'] = models_dir
        job['models_yyyy_mm_nn_dir'] = join(job['models_dir'], job['yyyymmnn_dir'])
        job['models_path'] = join(job['models_yyyy_mm_nn_dir'], job['base_name']+'_models.h5')
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

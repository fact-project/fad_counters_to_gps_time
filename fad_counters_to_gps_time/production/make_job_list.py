import os
import numpy as np
from os.path import abspath
from os.path import join
from os.path import exists


OBSERVATION_RUN_KEY = 1


def make_job_list(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fact_raw_dir='/fact/raw',
    tmp_dir_base_name='fact_fad_counter_to_gps_',
):
    """
    Returns a list of jobs, and output directory structure. 
    A job is a dict with all paths needed to extract the FAD counters from a
    FACT run and write all the stdout and stderror.

    Parameters
    ----------

    out_dir             The path to the output directory where the fad_counters
                        are collected. The out_dir is created if not existing.

    run_info            A pandas DataFrame() of the FACT run-info-database which
                        is used as a reference for the runs to be processed.
                        (default None, download the latest run-info on the fly)

    only_a_fraction     A ratio between 0.0 and 1.0 to only process a 
                        random fraction of the runs. Usefull for debugging over 
                        long periodes of observations. (default 1.0)

    fact_raw_dir        The path to the FACT raw observation directory.

    tmp_dir_base_name   The base name of the temporary directory on the qsub 
                        worker nodes. (default 'fact_photon_stream_JOB_ID_')
    """
    
    print('Start FAD counter extraction ...')

    out_dir = os.path.abspath(out_dir)
    fact_raw_dir = abspath(fact_raw_dir)
    
    std_dir = join(out_dir, 'std')
    phs_dir = join(out_dir, 'fad')
    job_dir = join(out_dir, 'job')

    directory_structure = {
        'out_dir': out_dir,
        'std_dir': std_dir,
        'fad_dir': phs_dir,
        'job_dir': job_dir,
    }

    jobs = observation_runs_in_run_info(
        run_info=run_info,
        only_a_fraction=only_a_fraction
    )

    print('Got '+str(len(jobs))+' observation runs to be processed.')
    print('Find overlap with runs accessible in "'+fact_raw_dir+'" ...')

    for job in jobs:
        job['yyyy'] = night_id_2_yyyy(job['Night'])
        job['mm'] = night_id_2_mm(job['Night'])
        job['nn'] = night_id_2_nn(job['Night'])
        job['fact_raw_dir'] = fact_raw_dir
        job['yyyymmnn_dir'] = '{y:04d}/{m:02d}/{n:02d}/'.format(
            y=job['yyyy'],
            m=job['mm'],
            n=job['nn']
        )
        job['base_name'] = '{bsn:08d}_{rrr:03d}'.format(
            bsn=job['Night'],
            rrr=job['Run']
        )
        job['raw_file_name'] = job['base_name']+'.fits.fz'
        job['raw_path'] = join(
            job['fact_raw_dir'], 
            job['yyyymmnn_dir'], 
            job['raw_file_name']
        )

    accesible_jobs = []
    for job in jobs:
        if os.path.exists(job['raw_path']):
            accesible_jobs.append(job)
    jobs = accesible_jobs

    print('Found '+str(len(jobs))+' runs both in run_info and accesible in "'+fact_raw_dir+'".')

    for job in jobs: 
        job['std_dir'] = std_dir
        job['std_yyyy_mm_nn_dir'] = join(job['std_dir'], job['yyyymmnn_dir'])
        job['std_out_path'] = join(job['std_yyyy_mm_nn_dir'], job['base_name']+'.o')
        job['std_err_path'] = join(job['std_yyyy_mm_nn_dir'], job['base_name']+'.e')

        job['job_yyyy_mm_nn_dir'] = join(job['job_dir'], job['yyyymmnn_dir'])
        job['job_path'] = join(job['job_yyyy_mm_nn_dir'], job['base_name']+'.sh')

        job['worker_tmp_dir_base_name'] = tmp_dir_base_name

        job['fad_dir'] = fad_dir
        job['fad_yyyy_mm_nn_dir'] = join(job['fad_dir'], job['yyyymmnn_dir'])
        job['fad_path'] = join(job['fad_yyyy_mm_nn_dir'], job['base_name']+'_fad.h5')
    
    print('Done.')

    return {
        'jobs': jobs,
        'directory_structure': directory_structure
    }



def observation_runs_in_run_info(
    run_info,
    only_a_fraction=1.0
):
    valid = (
        run_info['fRunTypeKey'] == OBSERVATION_RUN_KEY
    ).as_matrix()

    fraction = np.random.uniform(size=len(valid)) < only_a_fraction
    
    night_ids = run_info['fNight'][valid*fraction]
    run_ids = run_info['fRunID'][valid*fraction]

    jobs = []

    for i, run_id in enumerate(run_ids):
        jobs.append(
            {
                'Night': night_ids.iloc[i],
                'Run': run_id
            }
        )
    return jobs


def night_id_2_yyyy(night):
    return night // 10000


def night_id_2_mm(night):
    return (night // 100) % 100


def night_id_2_nn(night):
    return night % 100
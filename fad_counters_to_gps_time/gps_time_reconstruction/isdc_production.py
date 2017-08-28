import os
from os.path import abspath
from os.path import join
from os.path import exists
from os.path import dirname
import shutil
import pkg_resources
from shutil import which
import subprocess as sp
import pandas as pd
from tqdm import tqdm
from fact.path import TreePath
from fact.credentials import create_factdb_engine


OBSERVATION_RUN_KEY = 1


def copy_top_level_readme_to(path):
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        'gps_time_reconstruction/README.md'
    )
    shutil.copy(readme_res_path, path)


def make_job_list(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fad_counter_dir='/gpfs0/fact/processing/fad_counters/fad',
    tmp_dir_base_name='gps_time_reco_',
):
    out_dir = abspath(out_dir)
    fad_counter_dir = abspath(fad_counter_dir)

    jobs = run_info[
        run_info.fRunTypeKey == OBSERVATION_RUN_KEY
        ].sample(frac=only_a_fraction).copy()

    path_generators = {
        'input_file_path': TreePath(base_dir=fad_counter_dir, suffix='_fad.h5'),
        'std_out_path': TreePath(join(out_dir, 'std'), suffix='.o'),
        'std_err_path': TreePath(join(out_dir, 'std'), suffix='.e'),
        'gps_time_path': TreePath(join(out_dir, 'gps_time'), suffix='_gps_time.h5'),
        'models_path': TreePath(join(out_dir, 'gps_time_models'), suffix='_models.h5'),
    }

    for job in jobs.itertuples():
        for name, generator in path_generators:
            jobs.set(job.Index, name, generator(job.fNight, job.fRunID))

    jobs['input_file_exists'] = jobs.input_file_path.apply(exists)

    return jobs[jobs.input_file_exists]


def qsub(jobs, queue='fact_medium'):

    for job in tqdm(jobs.itertuples()):
        os.makedirs(dirname(job.std_out_path), exist_ok=True)
        os.makedirs(dirname(job.std_err_path), exist_ok=True)

        cmd = [
            'qsub',
            '-q', queue,
            '-o', job.std_out_path,
            '-e', job.std_err_path,
            which('gps_time_reconstruction'),
            job.input_file_path,
            job.gps_time_path,
            job.models_path,
        ]

        try:
            sp.check_output(cmd, stderr=sp.STDOUT)
        except sp.CalledProcessError as e:
            print('returncode', e.returncode)
            print('output', e.output)
            raise


def start_production_at_isdc():
    out_dir = '/gpfs0/fact/processing/gps_time'
    os.makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))
    production_store_path = join(out_dir, 'production_store.h5')

    if not exists(production_store_path):
        runinfo = pd.read_sql_table(
            'RunInfo',
            create_factdb_engine())
        runinfo['submission_time'] = pd.Timestamp(float('nan'))
        runinfo.to_hdf(production_store_path, 'all')

    production_store = pd.read_hdf(production_store_path)
    jobs = make_job_list(out_dir, production_store)

    qsub(jobs)

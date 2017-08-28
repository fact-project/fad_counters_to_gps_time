#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
    -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
"""
import os
from os.path import abspath
from os.path import join
from os.path import exists
from os.path import dirname
from datetime import datetime
import shutil
import pkg_resources
from shutil import which
import subprocess as sp
import pandas as pd
from tqdm import tqdm
from fact.path import TreePath
from fact.credentials import create_factdb_engine
from docopt import docopt
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(message)s'
)

OBSERVATION_RUN_KEY = 1


def copy_top_level_readme_to(path):
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        'gps_time_reconstruction/README.md'
    )
    shutil.copy(readme_res_path, path)


def assign_paths_to_runinfo(run_info, input_dir, out_dir):
    path_generators = {
        'input_file_path': TreePath(base_dir=input_dir, suffix='_fad.h5'),
        'std_out_path': TreePath(join(out_dir, 'std'), suffix='.o'),
        'std_err_path': TreePath(join(out_dir, 'std'), suffix='.e'),
        'gps_time_path':
            TreePath(join(out_dir, 'gps_time'), suffix='_gps_time.h5'),
        'models_path':
            TreePath(join(out_dir, 'gps_time_models'), suffix='_models.h5'),
    }

    for job in run_info.itertuples():
        for name, generator in path_generators.items():
            run_info.set_value(
                job.Index,
                name,
                generator(job.fNight, job.fRunID)
            )

    return run_info


def qsub(job, queue='fact_medium'):
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


def status(job):
    try:
        df = pd.read_hdf(job.gps_time_path)
        return len(df) == job.fNumEvents
    except:
        return False


def main():
    args = docopt(__doc__)
    logging.info(str(args))

    out_dir = abspath(args['--output'])
    input_dir = abspath(args['--input'])

    os.makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))

    logging.info('downloading list of observation runs ... ')
    runinfo = pd.read_sql(
        '''
        SELECT
            fNight, fRunID, fNumEvents
        FROM
            RunInfo
        WHERE
            fRunTypeKey={0}
        '''.format(OBSERVATION_RUN_KEY),
        create_factdb_engine()
    )
    logging.info(
        '{0} observation runs found in RunInfo DB'.format(
            len(runinfo)))

    runinfo = assign_paths_to_runinfo(runinfo, input_dir, out_dir)
    runinfo['input_file_exists'] = runinfo.input_file_path.apply(exists)
    runinfo['output_already_exists'] = runinfo.gps_time_path.apply(exists)
    runinfo['is_output_status_ok'] = runinfo.apply(status)
    runinfo['submitted_at'] = pd.Timestamp('nat')

    runs_with_input = runinfo[runinfo.input_file_exists]
    runs_without_output = runs_with_input[~runs_with_input.is_output_status_ok]

    for job in tqdm(
        runs_without_output.itertuples(),
        total=len(runs_without_output)
    ):
        qsub(job)
        runinfo.set_value(
            job.Index,
            'submitted_at',
            datetime.utcnow()
        )

    runinfo.to_hdf(join(out_dir, 'runinfo.h5'), 'all')

if __name__ == '__main__':
    main()

#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
    -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
    --init            initialize the runinfo storage for this processing
"""
import os
import sys
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
import numpy as np

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(message)s'
)

OBSERVATION_RUN_KEY = 1
SQL_QUERY = '''
SELECT
    fNight, fRunID, fNumEvents
FROM
    RunInfo
WHERE
    fRunTypeKey={0}
'''.format(OBSERVATION_RUN_KEY)

def copy_top_level_readme_to(path):
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        'gps_time_reconstruction/README.md'
    )
    shutil.copy(readme_res_path, path)


def assign_paths_to_runinfo(runinfo, input_dir, out_dir):

    path_generators = {
        'input_file_path': TreePath(base_dir=input_dir, suffix='_fad.h5'),
        'std_out_path': TreePath(join(out_dir, 'std'), '.o'),
        'std_err_path': TreePath(join(out_dir, 'std'), '.e'),
        'gps_time_path':
            TreePath(join(out_dir, 'gps_time'), '_gps_time.h5'),
        'models_path':
            TreePath(join(out_dir, 'gps_time_models'), '_models.h5'),
    }

    have_no_paths = runinfo[~runinfo.has_paths]
    for job in tqdm(
            have_no_paths.itertuples(),
            desc='assign_paths:',
            total=len(have_no_paths)
    ):
        for name, generator in path_generators.items():
            runinfo.set_value(
                job.Index,
                name,
                generator(job.fNight, job.fRunID)
            )
    runinfo['has_paths'] = True
    return runinfo


def check_for_input_files(runinfo):
    no_input_files = runinfo[~runinfo.input_file_exists]
    for job in tqdm(
            no_input_files.itertuples(),
            desc='check_for_input_file:',
            total=len(no_input_files)
    ):
        runinfo.set_value(
            job.Index,
            'input_file_exists',
            exists(job.input_file_path)
        )
    return runinfo


def check_for_output_files(runinfo):
    no_input_files = runinfo[~runinfo.output_file_exists]
    for job in tqdm(
            no_input_files.itertuples(),
            desc='check_for_ouput_file:',
            total=len(no_input_files)
    ):
        runinfo.set_value(
            job.Index,
            'output_file_exists',
            exists(job.gps_time_path)
        )
    return runinfo


def check_length_of_output(runinfo):
    to_be_checked = runinfo[
        runinfo.output_file_exists & np.isnan(runinfo.length_of_output)
    ]
    for job in tqdm(
            to_be_checked.itertuples(),
            desc='check_output_length:',
            total=len(to_be_checked)
    ):
        try:
            runinfo.set_value(
                job.Index,
                'length_of_output',
                float(len(pd.read_hdf(job.gps_time_path)))
            )
        except KeyboardInterrupt:
            raise
        except:
            logging.exception('in check_length_of_output')

    return runinfo


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


def update_runinfo(path):
    logging.info('downloading list of observation runs ... ')
    runinfo = pd.read_sql(SQL_QUERY, create_factdb_engine())
    runinfo = runinfo.merge(
        pd.read_hdf(path),
        on=list(runinfo.columns),
        how='outer',
    )
    runinfo.has_paths.fillna(False, inplace=True)
    runinfo.input_file_exists.fillna(False, inplace=True)
    runinfo.output_file_exists.fillna(False, inplace=True)
    return runinfo


def initialize_runinfo(path):
    runinfo = pd.read_sql(SQL_QUERY, create_factdb_engine())
    runinfo['has_paths'] = False
    runinfo['input_file_exists'] = False
    runinfo['output_file_exists'] = False
    runinfo['length_of_output'] = np.nan
    runinfo['submitted_at'] = pd.Timestamp('nat')

    return runinfo


def main():
    args = docopt(__doc__)
    out_dir = abspath(args['--output'])
    input_dir = abspath(args['--input'])
    runinfo_path = join(out_dir, 'runinfo.h5')
    os.makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))

    if args['--init']:
        runinfo = initialize_runinfo(runinfo_path)

    if not exists(runinfo_path):
        logging.error('runinfo file does not exist. Call with --init first')
        sys.exit(-1)

    runinfo = update_runinfo(runinfo_path)
    runinfo = assign_paths_to_runinfo(runinfo, input_dir, out_dir)
    runinfo = check_for_input_files(runinfo)
    runinfo = check_for_output_files(runinfo)
    runinfo = check_length_of_output(runinfo)

    runs_not_yet_submitted = runinfo[
        runinfo.input_file_exists &
        (~runinfo.output_file_exists) &
        np.isnat(runinfo.submitted_at)
    ]

    for job in tqdm(
        runs_not_yet_submitted.itertuples(),
        desc='submitting:',
        total=len(runs_not_yet_submitted)
    ):
        qsub(job)
        runinfo.set_value(
            job.Index,
            'submitted_at',
            datetime.utcnow()
        )

    runinfo.to_hdf(runinfo_path, 'all')

if __name__ == '__main__':
    main()

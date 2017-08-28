#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
    -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
    --init            initialize the runinfo storage for this processing
    --update          update the runingfo storage for this processing from DB
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
    for job in tqdm(have_no_paths.itertuples(), total=len(have_no_paths)):
        for name, generator in path_generators.items():
            runinfo.set_value(
                job.Index,
                name,
                generator(job.fNight, job.fRunID)
            )

    return runinfo


def check_for_input_files(runinfo):
    no_input_files = runinfo[~runinfo.input_file_exists]
    for job in tqdm(
            no_input_files.itertuples(),
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
            total=len(to_be_checked)
    ):
        try:
            runinfo.set_value(
                job.Index,
                'length_of_output',
                float(len(pd.read_hdf(job.gps_time_path)))
            )
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
    old_runinfo = pd.read_hdf(path)
    db = create_factdb_engine()
    new_runinfo = pd.read_sql(
        '''
        SELECT
            fNight, fRunID, fNumEvents
        FROM
            RunInfo
        WHERE
            fRunTypeKey={0}
        '''.format(OBSERVATION_RUN_KEY),
        db
    )
    new_runinfo = new_runinfo.merge(
        old_runinfo,
        on=['fNight', 'fRunID'])
    new_runinfo.to_hdf(path, 'all')


def initialize_runinfo(path):
    db = create_factdb_engine()
    runinfo = pd.read_sql(
        '''
        SELECT
            fNight, fRunID, fNumEvents
        FROM
            RunInfo
        WHERE
            fRunTypeKey={0}
        '''.format(OBSERVATION_RUN_KEY),
        db
    )
    runinfo['has_paths'] = False
    runinfo['input_file_exists'] = False
    runinfo['output_file_exists'] = False
    runinfo['length_of_output'] = np.nan
    runinfo['submitted_at'] = pd.Timestamp('nat')

    return runinfo


def main():
    args = docopt(__doc__)
    logging.info(str(args))
    out_dir = abspath(args['--output'])
    input_dir = abspath(args['--input'])
    runinfo_path = join(out_dir, 'runinfo.h5')
    os.makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))

    if args['--init']:
        runinfo = initialize_runinfo(runinfo_path)
        runinfo = assign_paths_to_runinfo(runinfo, input_dir, out_dir)
        runinfo = check_for_input_files(runinfo)
        runinfo = check_for_output_files(runinfo)
        runinfo = check_length_of_output(runinfo)
        runinfo.to_hdf(runinfo_path, 'all')

    if not exists(runinfo_path):
        logging.error('runinfo file does not exist. Call with --init first')
        sys.exit(-1)

    if args['--update']:
        update_runinfo(runinfo_path)

    runinfo = pd.read_hdf(runinfo_path)
    logging.info(
        '{0} runinfo'.format(
            len(runinfo)))

    runs_with_input = runinfo[runinfo.input_file_exists]
    logging.info(
        '{0} runs_with_input'.format(
            len(runs_with_input)))

    runs_with_output = runs_with_input[runs_with_input.output_file_exists]
    logging.info(
        '{0} runs_with_output'.format(
            len(runs_with_output)))

    runs_without_output = runs_with_input[~runs_with_input.output_file_exists]
    logging.info(
        '{0} runs_without_output'.format(
            len(runs_without_output)))

    runs_not_yet_submitted = runs_without_output[np.isnat(runs_without_output.submitted_at)]
    logging.info(
        '{0} runs_not_yet_submitted'.format(
            len(runs_not_yet_submitted)))

    to_bet_submitted = runs_not_yet_submitted.sample(frac=0.1)
    for job in tqdm(
        to_bet_submitted.itertuples(),
        total=len(to_bet_submitted)
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

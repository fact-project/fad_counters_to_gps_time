#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
    -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
"""
import os
from docopt import docopt
from os import remove
from os.path import exists
from os.path import dirname
from os.path import join
from os.path import abspath
from fact.path import tree_path
from functools import partial
from shutil import which
import subprocess as sp
from .production import main as production_main

from . import run_fad_counter_extraction


def init_path_generators(input_dir, out_dir):
    return {
        'input_file_path': partial(tree_path, input_dir, '_fad.h5'),
        'std_out_path': partial(tree_path, join(out_dir, 'std'), '.o'),
        'std_err_path': partial(tree_path, join(out_dir, 'std'), '.e'),
        'output_file_path':
            partial(tree_path, join(out_dir, 'gps_time'), '_gps_time.h5'),
        'models_path':
            partial(tree_path, (out_dir, 'gps_time_models'), '_models.h5'),
    }


def qsub(job, queue='fact_medium'):

    for p in [
        job.std_out_path,
        job.std_err_path
    ]:
        if exists(p):
            remove(p)
        else:
            os.makedirs(dirname(p), exist_ok=True)

    cmd = [
        'qsub',
        '-q', queue,
        '-o', job.std_out_path,
        '-e', job.std_err_path,
        which('gps_time_reconstruction'),
        job.input_file_path,
        job.output_file_path,
        job.models_path,
    ]

    try:
        sp.check_output(cmd, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        print('returncode', e.returncode)
        print('output', e.output)
        raise


def main():
    args = docopt(__doc__)
    out_dir = abspath(args['--output'])
    input_dir = abspath(args['--input'])
    path_generators = init_path_generators(input_dir, out_dir)

    production_main(path_generators, qsub, out_dir)


if __name__ == '__main__':
    main()

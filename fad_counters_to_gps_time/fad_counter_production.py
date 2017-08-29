#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/fad_counter_new]
    -i, --input DIR   input directory [default: /fact/raw]
    --init            initialize the runinfo storage for this processing
    --qsub            use qsub for submission
"""
import os
from docopt import docopt
from os import remove
from os.path import exists
from os.path import dirname
from os.path import join
from fact.path import tree_path
from functools import partial
from shutil import which
import subprocess as sp
from ..production import main as production_main

from . import run_fad_counter_extraction
import scoop


def init_path_generators(input_dir, out_dir):
    return {
        'input_file_path': partial(tree_path, base_dir=input_dir, suffix='.fits.fz'),
        'std_out_path': partial(tree_path, join(out_dir, 'std'), '.o'),
        'std_err_path': partial(tree_path, join(out_dir, 'std'), '.e'),
        'output_file_path':
            partial(tree_path, join(out_dir, 'gps_time'), '_fad.h5'),
    }


def scoop_submit(job):
    scoop.futures.submit(
        run_fad_counter_extraction,
        {
            'in_path': job.input_file_path,
            'out_path': job.output_file_path,
        }
    )


def qsub(job, queue='fact_medium'):

    for p in [
        job.std_out_path,
        job.std_err_path,
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
        which('fad_counter_extraction'),
        job.input_file_path,
        job.output_file_path,
    ]

    try:
        sp.check_output(cmd, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        print('returncode', e.returncode)
        print('output', e.output)
        raise


def main():
    args = docopt(__doc__)
    if args['--qsub']:
        production_main(
            init_path_generators,
            qsub)
    else:
        production_main(
            init_path_generators,
            scoop_submit)


if __name__ == '__main__':
    main()

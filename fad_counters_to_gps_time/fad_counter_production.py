#!/bin/env python

from os.path import (join, dirname, realpath)
from os import makedirs
import shutil

from fact.path import tree_path
from functools import partial
import manure

input_dir = '/fact/raw'
out_dir = '/gpfs0/fact/processing/public/fad_counter'
makedirs(out_dir, exist_ok=True)

shutil.copy(
    join(dirname(realpath(__file__)), 'README.md'),
    join(out_dir, 'README.md')
)

manure.production_main(
    path_generators={
        'input_file_path': partial(
            tree_path,
            prefix=input_dir,
            suffix='.fits.fz'),
        'std_out_path': partial(
            tree_path,
            prefix=out_dir,
            suffix='.o'),
        'std_err_path': partial(
            tree_path,
            prefix=out_dir,
            suffix='.e'),
        'output_file_path': partial(
                tree_path,
                prefix=out_dir,
                suffix='_fad.h5'),
    },
    submission=partial(
        manure.qsub,
        o_path='std_out_path',
        e_path='std_err_path',
        binary_name='single_fad_counter_extraction',
        args=['input_file_path', 'output_file_path']
    ),
    runstatus_path=join(out_dir, 'runstatus.csv')
)

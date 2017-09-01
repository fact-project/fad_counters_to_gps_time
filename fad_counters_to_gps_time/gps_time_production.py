#!/bin/env python

from os.path import (join, dirname, realpath)
import shutil

from fact.path import tree_path
from functools import partial
import manure

input_dir = '/gpfs0/fact/processing/fad_counters/fad'
out_dir = '/gpfs0/fact/processing/gps_time'

shutil.copy(
    join(dirname(realpath(__file__)), 'README.md'),
    join(out_dir, 'README.md')
)

manure.production_main(
    path_generators={
        'input_file_path': partial(
            tree_path,
            prefix=input_dir,
            suffix='_fad.h5'),
        'std_out_path': partial(
            tree_path,
            prefix=join(out_dir, 'std'),
            suffix='.o'),
        'std_err_path': partial(
            tree_path,
            prefix=join(out_dir, 'std'),
            suffix='.e'),
        'output_file_path': partial(
            tree_path,
            prefix=join(out_dir, 'gps_time'),
            suffix='_gps_time.h5'),
        'models_path': partial(
            tree_path,
            prefix=join(out_dir, 'gps_time'),
            suffix='_gps_time_models.h5'),
    },
    submission=partial(
        manure.qsub,
        o_path='std_out_path',
        e_path='std_err_path',
        binary_name='gps_time_reconstruction',
        args=['input_file_path', 'output_file_path', 'models_path'],
    ),
    runstatus_path=join(out_dir, 'runstatus.csv')
)

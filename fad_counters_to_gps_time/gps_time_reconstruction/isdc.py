from tqdm import tqdm
import os
import os.path
from os.path import dirname
from .dummy_qsub import dummy_qsub
import subprocess as sp
from shutil import which
from ..copy_readmes import copy_top_level_readme_to


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

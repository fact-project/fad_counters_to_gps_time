from tqdm import tqdm
import os
import os.path
from .dummy_qsub import dummy_qsub
import subprocess as sp
from shutil import which
from ..copy_readmes import copy_top_level_readme_to


def qsub(jobs, queue='fact_medium', use_dummy_qsub=False):

    os.makedirs(jobs[0]['out_dir'], exist_ok=True)
    copy_top_level_readme_to(os.path.join(jobs[0]['out_dir'], 'README.md'))

    for job in tqdm(jobs):
        os.makedirs(os.path.split(job['std_out_path'])[0], exist_ok=True)
        os.makedirs(os.path.split(job['std_err_path'])[0], exist_ok=True)

        cmd = [
            'qsub',
            '-q', queue,
            '-o', job['std_out_path'],
            '-e', job['std_err_path'],
            which('gps_time_reconstruction'),
            job['input_file_path'],
            job['gps_time_path'],
            job['models_path'],
        ]

        if use_dummy_qsub:
            dummy_qsub(cmd)
        else:
            try:
                sp.check_output(cmd, stderr=sp.STDOUT)
            except sp.CalledProcessError as e:
                print('returncode', e.returncode)
                print('output', e.output)
                raise

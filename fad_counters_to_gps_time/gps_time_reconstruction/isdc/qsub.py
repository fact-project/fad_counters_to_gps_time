from tqdm import tqdm
import os
import os.path
from ..make_job_list import make_job_list
from .dummy_qsub import dummy_qsub
import subprocess as sp
from ..copy_readmes import copy_top_level_readme_to
import pkg_resources

def qsub(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fad_counter_dir='/gpfs0/fact/processing/fad_counters/fad',
    tmp_dir_base_name='gps_time_reco_',
    queue='fact_medium',
    use_dummy_qsub=False,
):
    jobs = make_job_list(
        out_dir=out_dir,
        run_info=run_info,
        only_a_fraction=only_a_fraction,
        fad_counter_dir=fad_counter_dir,
        tmp_dir_base_name=tmp_dir_base_name,
    )
    os.makedirs(os.path.abspath(out_dir), exist_ok=True)
    copy_top_level_readme_to(os.path.join(out_dir, 'README.md'))

    for job in tqdm(jobs):
        os.makedirs(os.path.split(job['std_out_path'])[0], exist_ok=True)
        os.makedirs(os.path.split(job['std_err_path'])[0], exist_ok=True)

        script_path = pkg_resources.resource_filename(
            'fad_counters_to_gps_time',
            'gps_time_reconstruction/__init__.py'
        )

        cmd = [
            'qsub',
            '-q', queue,
            '-o', job['std_out_path'],
            '-e', job['std_err_path'],
            script_path,
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

from tqdm import tqdm
import os
from ..make_job_list import make_job_list
from .dummy_qsub import dummy_qsub
import subprocess as sp
from ..copy_readmes import copy_top_level_readme_to


def qsub(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fad_counter_dir='/gpfs0/fact/processing/fad_counters/fad',
    tmp_dir_base_name='gps_time_reco_',
    queue='fact_medium',
    use_dummy_qsub=False,
):
    job_structure = make_job_list(
        out_dir=out_dir,
        run_info=run_info,
        only_a_fraction=only_a_fraction,
        fad_counter_dir=fad_counter_dir,
        tmp_dir_base_name=tmp_dir_base_name,
    )

    jobs = job_structure['jobs']
    out_dirs = job_structure['directory_structure']
    for out_dir in out_dirs.values():
        os.makedirs(out_dir, exist_ok=True)

    copy_top_level_readme_to(os.path.join(out_dirs['out_dir'], 'README.md'))

    for job in tqdm(jobs):
        os.makedirs(job['std_yyyy_mm_nn_dir'], exist_ok=True)
        os.makedirs(job['gps_time_yyyy_mm_nn_dir'], exist_ok=True)
        os.makedirs(job['models_yyyy_mm_nn_dir'], exist_ok=True)

        cmd = [
            'qsub',
            '-q', queue,
            '-o', job['std_out_path'],
            '-e', job['std_err_path'],
            'gps_time_reconstruction {fad_counter_path} {gps_time_path} {models_path}'.format(
                fad_counter_path=job['input_file_path'],
                gps_time_path=job['gps_time_path'],
                models_path=job['models_path']
                )
        ]

        if use_dummy_qsub:
            dummy_qsub(cmd)
        else:
            qsub_return_code = sp.call(cmd)
            if qsub_return_code > 0:
                print('qsub return code: ', qsub_return_code)

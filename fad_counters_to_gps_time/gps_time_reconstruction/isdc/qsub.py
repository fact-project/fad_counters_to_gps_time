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
        os.makedirs(job['std_yyyy_mm_nn_dir'], exist_ok=True)
        os.makedirs(job['gps_time_yyyy_mm_nn_dir'], exist_ok=True)
        os.makedirs(job['models_yyyy_mm_nn_dir'], exist_ok=True)

        cmd = [
            'qsub',
            '-q', queue,
            '-o', job['std_out_path'],
            '-e', job['std_err_path'],
            sp.check_output(['which', 'gps_time_reconstruction']).split(),
            job['input_file_path'],
            job['gps_time_path'],
            job['models_path'],
        ]

        if use_dummy_qsub:
            dummy_qsub(cmd)
        else:
            qsub_return_code = sp.call(cmd)
            if qsub_return_code > 0:
                print('qsub return code: ', qsub_return_code)

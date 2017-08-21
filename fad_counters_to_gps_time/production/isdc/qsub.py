from tqdm import tqdm
import os
from ..make_job_list import make_job_list
from .write_worker_node_script import write_worker_node_script


def qsub(
    out_dir,
    run_info,
    only_a_fraction=1.0,
    fact_raw_dir='/fact/raw',
    tmp_dir_base_name='fact_fad_counter_to_gps_',
    queue='fact_medium', 
    email='sebmuell@phys.ethz.ch',
):
    job_structure = make_job_list(
        out_dir=out_dir,
        only_a_fraction=only_a_fraction,
        fact_raw_dir=fact_raw_dir,
        tmp_dir_base_name=tmp_dir_base_name,
        run_info=run_info,
    )

    jobs = job_structure['jobs']
    out_dirs = job_structure['directory_structure']
    os.makedirs(out_dirs['out_dir'], exist_ok=True)
    os.makedirs(out_dirs['fad_dir'], exist_ok=True)
    os.makedirs(out_dirs['std_dir'], exist_ok=True)
    os.makedirs(out_dirs['job_dir'], exist_ok=True)

    for job in tqdm(jobs):
        os.makedirs(job['job_yyyy_mm_nn_dir'], exist_ok=True)
        os.makedirs(job['std_yyyy_mm_nn_dir'], exist_ok=True)

        write_worker_node_script(
            path=job['job_path'],
            input_run_path=job['raw_path'],
            output_fad_counter_path=job['fad_path']
        )

        cmd = [ 
            'qsub',
            '-q', queue,
            '-o', job['std_out_path'],
            '-e', job['std_err_path'],
            '-m', 'ae', # send email in case of (e)nd or (a)bort
            '-M', email,
            job['job_path']
        ]
   
        qsub_return_code = sp.call(cmd)
        if qsub_return_code > 0:
            print('qsub return code: ', qsub_return_code)

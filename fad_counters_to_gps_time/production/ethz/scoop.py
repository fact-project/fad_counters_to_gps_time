"""
Usage: scoop_produce_phs --out_dir=DIR [--start_night=NIGHT] [--end_night=NIGHT] --fact_raw_dir=DIR --fact_drs_dir=DIR --fact_aux_dir=DIR --fact_tools_jar_path=PATH --fact_tools_xml_path=PATH --java_path=PATH [--tmp_dir_base_name=BASE] [--only_a_fraction=FACTOR] (--run_info_path=PATH | --fact_password=PASSWORD)

Options:
    --out_dir=DIR
    --only_a_fraction=FACTOR    [default: 1.0]
    --fact_raw_dir=DIR          [default: /data/fact_data]
    --tmp_dir_base_name=BASE    [default: fact_fad_counter_]  
    --run_info_path=PATH
"""
import docopt
import scoop
import os
import glob
import fad_counters_to_gps_time as fad2gps
from os.path import join
from os.path import split
from os.path import exists
import subprocess
import tempfile
import shutil


def run_fad_extraction_job(job):
    os.makedirs(job['job_yyyy_mm_nn_dir'], exist_ok=True)
    os.makedirs(job['std_yyyy_mm_nn_dir'], exist_ok=True)
    os.makedirs(job['fad_yyyy_mm_nn_dir'], exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=job['worker_tmp_dir_base_name']) as tmp:
        input_run_base = split(job['raw_path'])[1]
        tmp_input_run_path = join(tmp, input_run_base)
        shutil.copy(job['raw_path'], tmp_input_run_path)
        fad_counters = fad2gps.read_fad_counters(path=tmp_input_run_path)
        fad_counters.to_hdf(job['fad_path'], 'all')
    return 0


def main():
    try:
        arguments = docopt.docopt(__doc__)

        runinfo = ps.production.runinfo.read_runinfo_from_file(
            arguments['--run_info_path']
        )

        job_structure = fad2gps.production.prepare.make_job_list(
            out_dir=arguments['--out_dir'],
            run_info=runinfo,
            only_a_fraction=float(arguments['--only_a_fraction']),
            fact_raw_dir=arguments['--fact_raw_dir'],
            tmp_dir_base_name=arguments['--tmp_dir_base_name'],
        )

        jobs = job_structure['jobs']
        out_dirs = job_structure['directory_structure']
        os.makedirs(out_dirs['out_dir'], exist_ok=True)
        os.makedirs(out_dirs['fad_dir'], exist_ok=True)
        os.makedirs(out_dirs['std_dir'], exist_ok=True)
        os.makedirs(out_dirs['job_dir'], exist_ok=True)

        job_return_codes = list(scoop.futures.map(run_fad_extraction_job, jobs))

    except docopt.DocoptExit as e:
        print(e)

if __name__ == '__main__':
    main()

"""
Usage: scoop_produce_phs --out_dir=DIR --run_info_path=PATH [--fact_raw_dir=DIR] [--tmp_dir_base_name=BASE] [--only_a_fraction=FACTOR]

Options:
    --out_dir=DIR
    --run_info_path=PATH
    --only_a_fraction=FACTOR    [default: 1.0]
    --fact_raw_dir=DIR          [default: /data/fact_data]
    --tmp_dir_base_name=BASE    [default: fact_fad_counter_]
"""
import docopt
import scoop
import os
import fad_counters_to_gps_time as fad2gps
import pandas as pd


def run_fad_extraction_job(job):
    os.makedirs(job['fad_yyyy_mm_nn_dir'], exist_ok=True)
    fad2gps.production.run_fad_counter_extraction(
        in_path=job['raw_path'],
        out_path=job['fad_path']
    )
    return 0


def main():
    try:
        arguments = docopt.docopt(__doc__)

        runinfo = pd.read_msgpack(arguments['--run_info_path'])

        job_structure = fad2gps.production.make_job_list(
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
        fad2gps.production.copy_readmes.copy_top_level_readme_to(os.path.join(out_dirs['out_dir'], 'README.md'))

        job_return_codes = list(scoop.futures.map(run_fad_extraction_job, jobs))

    except docopt.DocoptExit as e:
        print(e)

if __name__ == '__main__':
    main()

#!/bin/env python
"""
Usage:
    scoop_production [options]

Options:
    -o, --output DIR  output_directory [default: /data/fact/fad_counter_new]
    -i, --input DIR   input directory [default: /fact/raw]
    --init            initialize the runinfo storage for this processing
"""
from ..production import main as production_main
import scoop
from . import run_fad_counter_extraction


def scoop_submit(job):
    scoop.futures.submit(
        run_fad_counter_extraction,
        {
            'in_path': job.input_file_path,
            'out_path': job.output_file_path,
        }
    )


def main():
    production_main(scoop_submit)

if __name__ == '__main__':
    main()

"""
Usage:
    fact_counter_extraction <input_path> <output_path> [options]

Options:
    --show_progress    show a progress bar

Extracts the FAD counters of each event from a FACT raw file and writes them
into a separate output file.
"""
import docopt
import shutil
from os.path import abspath
from ..read_fad_counters import read_fad_counters


def run_fad_counter_extraction(in_path, out_path, show_progress):
    fad_counters = read_fad_counters(
        path=in_path,
        show_progress=show_progress,
    )
    fad_counters.to_hdf(out_path+'.part', 'all')
    shutil.move(out_path+'.part', out_path)


def main():
    try:
        args = docopt.docopt(__doc__)
        run_fad_counter_extraction(
            in_path=abspath(args['<input_path>']),
            out_path=abspath(args['<output_path>']),
            show_progress=args['--show_progress'],
        )

    except docopt.DocoptExit as e:
        print(e)

if __name__ == '__main__':
    main()

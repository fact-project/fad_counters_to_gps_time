"""
Usage:
    fact_counter_extraction -i=PATH -o=PATH

Options:
    -i, --in_path=PATH       Path to a FACT raw fits run file.
    -o, --out_path=PATH      Path of the output run with only the fad counters.

Extracts the FAD counters of each event from a FACT raw file and writes them
into a separate output file.
"""
import docopt
import shutil
from ..read_fad_counters import read_fad_counters


def run_fad_counter_extraction(in_path, out_path):
    fad_counters = read_fad_counters(
        path=in_path,
        show_progress=False,
    )
    fad_counters.to_hdf(out_path+'.part', 'all')
    shutil.move(out_path+'.part', out_path)


def main():
    try:
        arguments = docopt.docopt(__doc__)
        in_path = arguments['--in_path']
        out_path = arguments['--out_path']
        run_fad_counter_extraction(in_path=in_path, out_path=out_path)

    except docopt.DocoptExit as e:
        print(e)

if __name__ == '__main__':
    main()

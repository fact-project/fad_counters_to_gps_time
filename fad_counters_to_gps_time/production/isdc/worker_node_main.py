"""
Usage: 
    fact_counter_extraction -i=PATH -o=PATH

Options:
    -i, --input_run_path=PATH
    -o, --out_path=PATH

Extracts the FAD counters of each event from a FACT raw file and writes them
into a separate output file.
"""
import docopt
import shutil 
import tempfile
from ...read_fad_counters import read_fad_counters
from os.path import join
from os.path import split


def main():
    try:
        arguments = docopt.docopt(__doc__)
        input_run_path = arguments['--input_run_path']
        out_path = arguments['--out_path']
          
        with tempfile.TemporaryDirectory(prefix='fact_fad_counters') as tmp:
            input_run_base = split(input_run_path)[1]
            tmp_input_run_path = join(tmp, input_run_base)
            shutil.copy(input_run_path, tmp_input_run_path)
            fad_counters = read_fad_counters(
                path=tmp_input_run_path, 
                show_progress=True,
            )
            fad_counters.to_hdf(out_path, 'all')

    except docopt.DocoptExit as e:
        print(e)

if __name__ == '__main__':
    main()

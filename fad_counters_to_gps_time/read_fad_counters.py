"""read_fad_counters.py

Read out the 40 FAD counters from a FACT zfits file and write them into a
pandas dataframe. When called as a standalone the dataframe is written into an
HDF5 file.

Usage:
  read_fad_counters.py <zfits_in_file_path> <h5_out_file_path>

Options:
  -h --help         Show this screen.
  --version         Show version.
  --ŝhow_progress   Show progress using tqdm
"""
from docopt import docopt
import zfits
import pandas as pd
from tqdm import trange


def read_fad_counters(path, show_progress=False):
    zfits_file = zfits.ZFits(path)
    data = []
    for event_id in trange(zfits_file['Events'].get_nrows()):

        event_num = zfits_file.get('Events', 'EventNum', event_id)[0]
        trigger_type = zfits_file.get('Events', 'TriggerType', event_id)[0]
        unix_time_tuple = zfits_file.get('Events', 'UnixTimeUTC', event_id)
        board_times = zfits_file.get('Events', 'BoardTime', event_id)

        for board_id in range(len(board_times)):
            data.append({
                'event_num': event_num,
                'trigger_type': trigger_type,
                'board_id': board_id,
                'counter': board_times[board_id],
                'unix_time': unix_time_tuple[0] + unix_time_tuple[1] / 1e6,
                })

    df = pd.DataFrame(data)
    df['night'] = zfits_file['Events'].read_header()['NIGHT']
    df['run_id'] = zfits_file['Events'].read_header()['RUNID']
    return df


if __name__ == '__main__':
    arguments = docopt(__doc__, version='read_fad_counters 0.0')
    print(arguments)
    df = read_fad_counters(
        arguments['<zfits_in_file_path>'],
        show_progress=arguments['--show_progress'],
    )

    df.to_hdf(arguments['<h5_out_file_path>'], 'all')

"""
Usage:
  single_fad_counter_extraction <input_path> <output_path> [options]

Options:
  --show    show a progress bar

Extracts the FAD counters of each event from a FACT raw file and writes them
into a separate output file.
"""
import docopt
import shutil
from os import makedirs
from os.path import abspath
from os.path import dirname
import zfits
import numpy as np
import pandas as pd
from tqdm import tqdm


def main():
    try:
        args = docopt.docopt(__doc__)
        run_fad_counter_extraction(
            in_path=abspath(args['<input_path>']),
            out_path=abspath(args['<output_path>']),
            show_progress=args['--show'],
        )

    except docopt.DocoptExit as e:
        print(e)


def run_fad_counter_extraction(in_path, out_path, show_progress=False):
    fad_counters = read_fad_counters(
        path=in_path,
        show_progress=show_progress,
    )

    makedirs(dirname(out_path), exist_ok=True)
    fad_counters.to_hdf(out_path+'.part', 'all')
    shutil.move(out_path+'.part', out_path)
    return 0  # for scoop


def read_fad_counters(path, show_progress=False):
    zfits_file = zfits.FactFits(path)
    data = []

    fNight = zfits_file.header()['NIGHT']
    fRunID = zfits_file.header()['RUNID']

    for event in tqdm(
            zfits_file,
            total=zfits_file.rows,
            disable=not show_progress
    ):

        Event = event.EventNum[0]
        Trigger = event.TriggerType[0]
        unix_time_tuple = event.UnixTimeUTC
        board_times = event.BoardTime

        d = {
            'fNight': fNight,
            'fRunID': fRunID,
            'Event': Event,
            'Trigger': Trigger,
            'UnixTime_ns': (
                int(1e9) * unix_time_tuple[0] +
                int(1e3) * unix_time_tuple[1])
        }
        for board_id, board_time in enumerate(board_times):
            d['Counter_{0:02d}'.format(board_id)] = board_time
        data.append(d)

    df = pd.DataFrame(data)
    df['fNight'] = df.fNight.astype(np.uint32)
    df['fRunID'] = df.fRunID.astype(np.uint32)
    df['Event'] = df.Event.astype(np.uint32)
    df['Trigger'] = df.Trigger.astype(np.uint16)
    df['UnixTime_ns'] = df.UnixTime_ns.astype(np.uint64)
    for board_id, board_time in enumerate(board_times):
        df['Counter_{0:02d}'.format(board_id)] = (
            df['Counter_{0:02d}'.format(board_id)].astype(np.uint32)
        )
    return df

if __name__ == '__main__':
    main()

import zfits
import numpy as np
import pandas as pd
from tqdm import trange


def read_fad_counters(path, show_progress=False):
    zfits_file = zfits.ZFits(path)
    data = []

    fNight = np.uint32(zfits_file['Events'].read_header()['NIGHT'])
    fRunID = np.uint32(zfits_file['Events'].read_header()['RUNID'])

    for event_id in trange(
            zfits_file['Events'].get_nrows(),
            disable=not show_progress
            ):

        Event = zfits_file.get('Events', 'EventNum', event_id)[0]
        Trigger = zfits_file.get('Events', 'TriggerType', event_id)[0]
        unix_time_tuple = zfits_file.get('Events', 'UnixTimeUTC', event_id)
        board_times = zfits_file.get('Events', 'BoardTime', event_id)

        d = {
            'fNight': fNight,
            'fRunID': fRunID,
            'Event': np.uint32(Event),
            'Trigger': np.uint16(Trigger),
            'UnixTime_ns': (
                np.uint64(1e9) * np.uint64(unix_time_tuple[0]) +
                np.uint64(1e3) * np.uint64(unix_time_tuple[1]))
        }
        for board_id in range(len(board_times)):
            d['Counter_{0}'.format(board_id)] = np.uint32(board_times[board_id])
        data.append(d)

    return pd.DataFrame(data)

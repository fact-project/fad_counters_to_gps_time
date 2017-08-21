import zfits
import pandas as pd
from tqdm import trange


def read_fad_counters(path, show_progress=False):
    zfits_file = zfits.ZFits(path)
    data = []
    for event_id in trange(
            zfits_file['Events'].get_nrows(),
            disable=not show_progress
            ):

        event_num = zfits_file.get('Events', 'EventNum', event_id)[0]
        trigger_type = zfits_file.get('Events', 'TriggerType', event_id)[0]
        unix_time_tuple = zfits_file.get('Events', 'UnixTimeUTC', event_id)
        board_times = zfits_file.get('Events', 'BoardTime', event_id)

        d = {
            'event_num': event_num,
            'trigger_type': trigger_type,
            'unix_time': unix_time_tuple[0] + unix_time_tuple[1] / 1e6,
        }
        for board_id in range(len(board_times)):
            d['counter_{0}'.format(board_id)] = board_times[board_id]
        data.append(d)

    df = pd.DataFrame(data)
    df['night'] = zfits_file['Events'].read_header()['NIGHT']
    df['run_id'] = zfits_file['Events'].read_header()['RUNID']
    return df

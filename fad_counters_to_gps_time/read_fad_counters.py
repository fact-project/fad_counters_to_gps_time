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

        Event = zfits_file.get('Events', 'EventNum', event_id)[0]
        Trigger = zfits_file.get('Events', 'TriggerType', event_id)[0]
        unix_time_tuple = zfits_file.get('Events', 'UnixTimeUTC', event_id)
        board_times = zfits_file.get('Events', 'BoardTime', event_id)

        d = {
            'Event': Event,
            'Trigger': Trigger,
            'UnixTime': unix_time_tuple[0] + unix_time_tuple[1] / 1e6,
        }
        for board_id in range(len(board_times)):
            d['Counter_{0}'.format(board_id)] = board_times[board_id]
        data.append(d)

    df = pd.DataFrame(data)
    df['Night'] = zfits_file['Events'].read_header()['NIGHT']
    df['Run'] = zfits_file['Events'].read_header()['RUNID']

    df['Event'] = df.Event.astype('u4')
    df['Trigger'] = df.Trigger.astype('u2')
    df['UnixTime'] = df.UnixTime.astype('f8')
    df['Night'] = df.Night.astype('u4')
    df['Run'] = df.Run.astype('u4')
    for board_id in range(40):
        df['Counter_{0}'.format(board_id)] = df['Counter_{0}'.format(board_id)].astype('u4')

    return df

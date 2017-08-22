import numpy as np
import pandas as pd
import os
from tqdm import tqdm
from ..production.make_job_list import OBSERVATION_RUN_KEY
from ..production.make_job_list import night_id_2_yyyy
from ..production.make_job_list import night_id_2_mm
from ..production.make_job_list import night_id_2_nn
from fact import credentials


def number_of_events_in_fad_counter_run(fad_run_path):
    fad_run = pd.read_hdf(fad_run_path)
    return len(ri['Event'])


def update_status_runinfo(fad_dir, runinfo):
    ri = runinfo

    if 'FadCounterNumEvents' not in ri:
        ri['FadCounterNumEvents'] = pd.Series(
            np.zeros(
                len(ri['fRunID']), 
                dtype=np.uint32
            ), 
            index=ri.index
        )

    for index, row in tqdm(ri.iterrows()):
        night = ri['fNight'][index]
        run = ri['fRunID'][index]

        if ri['fRunTypeKey'][index] == OBSERVATION_RUN_KEY:
            if ri['FadCounterNumEvents'][index] == 0:
                file_name = '{yyyymmnn:08d}_{rrr:03d}_fad.h5'.format(
                    yyyymmnn=night,
                    rrr=run
                )

                run_path = os.path.join(
                    fad_dir, 
                    '{yyyy:04d}'.format(yyyy=night_id_2_yyyy(night)), 
                    '{mm:02d}'.format(mm=night_id_2_mm(night)), 
                    '{nn:02d}'.format(nn=night_id_2_nn(night)), 
                    file_name
                )

                if os.path.exists(run_path):    
                    ri.set_value(
                        index, 
                        'FadCounterNumEvents', 
                        number_of_events_in_fad_counter_run(run_path)
                    )
                    print(
                        'New run '+str(night)+' '+str(run)+' '+
                        str(ri['FadCounterNumEvents'][index])+
                        ' events.'
                    )
    return ri


def update_known_runs(fad_dir, known_runs_file_name='known_runs.h5'):
    known_runs_path = join(fad_dir, known_runs_file_name)
    if os.path.exists(known_runs_path):
        known_runs = pd.read_hdf(known_runs_path)
    else:   
        known_runs = latest_runinfo()
    known_runs = update_status_runinfo(fad_dir=fad_dir, runinfo=known_runs) 
    known_runs.to_hdf(known_runs_path+'.part', 'all')
    shutil.move(known_runs_path+'.part', known_runs_path)


def latest_runinfo():
    factdb = credentials.create_factdb_engine()
    print("Reading fresh RunInfo table, takes about 1min.")
    return pd.read_sql_table("RunInfo", factdb)
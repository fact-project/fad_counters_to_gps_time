import pandas as pd
import os
import shutil
from tqdm import tqdm
from ..production.make_job_list import OBSERVATION_RUN_KEY
from fact import credentials


def make_run_path(run, base_dir, suffix='_fad.h5'):
    night = '{:08d}'.format(run.fNight)
    run_id = '{:03d}'.format(run.fRunID)

    return os.path.join(
        base_dir,
        '{yyyy}',
        '{mm}',
        '{nn}',
        '{night}_{run_id}{suffix}'
    ).format(
        night=night,
        run_id=run_id,
        yyyy=night[0:4],
        mm=night[4:6],
        nn=night[6:8],
        suffix=suffix,
    )


def update_status_runinfo(fad_dir, runinfo):
    if 'FadCounterNumEvents' not in runinfo:
        runinfo['FadCounterNumEvents'] = 0

    not_done_observation_runs = runinfo[
        (runinfo.FadCounterNumEvents == 0) &
        (runinfo.fRunTypeKey == OBSERVATION_RUN_KEY)
    ]

    for run in tqdm(not_done_observation_runs.itertuples()):
        run_path = make_run_path(run, base_dir=fad_dir)
        if os.path.exists(run_path):
            runinfo.set_value(
                run.Index,
                'FadCounterNumEvents',
                len(pd.read_hdf(run_path))
            )
    return runinfo


def update_known_runs(fad_dir, known_runs_file_name='known_runs.h5'):
    known_runs_path = os.path.join(fad_dir, known_runs_file_name)
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

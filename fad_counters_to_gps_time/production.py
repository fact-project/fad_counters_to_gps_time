from os import makedirs
from os.path import realpath
from os.path import join
from os.path import exists
from os.path import dirname
from os.path import split
from datetime import datetime
import shutil
import pkg_resources
import pandas as pd
from tqdm import tqdm
from fact.credentials import create_factdb_engine
import numpy as np

OBSERVATION_RUN_KEY = 1
SQL_QUERY = '''
SELECT
    fNight, fRunID, fNumEvents
FROM
    RunInfo
WHERE
    fRunTypeKey={0}
'''.format(OBSERVATION_RUN_KEY)


def copy_top_level_readme_to(path):
    this_sub_package_name = split(dirname(realpath(__file__)))[-1]
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        this_sub_package_name+'/README.md'
    )
    shutil.copy(readme_res_path, path)


def get_current_runstatus(runstatus_path):
    runinfo = pd.read_sql(SQL_QUERY, create_factdb_engine())
    if exists(runstatus_path):
        runinfo = runinfo.merge(
            pd.read_csv(path),
            on=list(runinfo.columns),
            how='outer',
        )
        runinfo.input_file_exists.fillna(False, inplace=True)
    else:
        runinfo['input_file_exists'] = False
        runinfo['submitted_at'] = pd.Timestamp('nat')
    return runinfo


def check_for_input_files(runinfo, path_generator):
    no_input_files = runinfo[~runinfo.input_file_exists]
    for job in tqdm(
            no_input_files.itertuples(),
            desc='check_for_input_file:',
            total=len(no_input_files)
    ):
        runinfo.set_value(
            job.Index,
            'input_file_exists',
            exists(path_generator(job))
        )


def production_main(
        path_gens,
        function_to_call_with_job,
        out_dir
):
    makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))

    runstatus_path = join(out_dir, 'runinfo.csv')
    runstatus = get_current_runstatus(runstatus_path)

    # Note: these modify runstatus in place
    check_for_input_files(runstatus, path_gens['input_file_path'])

    runs_not_yet_submitted = runstatus[
        runstatus.input_file_exists &
        np.isnat(runstatus.submitted_at)
    ].copy()

    for job in runs_not_yet_submitted.itertuples():
        for name, gen in path_gens:
            runs_not_yet_submitted.set_value(
                job.Index,
                name,
                gen(job)
            )

    for job in runs_not_yet_submitted.itertuples():
        runstatus.set_value(
            job.Index,
            'submitted_at',
            datetime.utcnow()
        )
        function_to_call_with_job(job)

    runstatus.to_csv(runstatus_path, 'all')

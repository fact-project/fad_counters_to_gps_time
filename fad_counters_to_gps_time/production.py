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


class RunStatus:

    def __init__(self, path):
        self.path = path
        self.runstatus = None

    def __enter__(self):
        runstatus = pd.read_sql(SQL_QUERY, create_factdb_engine())
        if exists(self.path):
            runstatus = runstatus.merge(
                pd.read_csv(self.path),
                on=list(runstatus.columns),
                how='outer',
            )
            runstatus.input_file_exists.fillna(False, inplace=True)
        else:
            runstatus['input_file_exists'] = False
            runstatus['submitted_at'] = pd.Timestamp('nat')

        self.runstatus = runstatus
        return self.runstatus

    def __exit__(self):
        self.runstatus.to_csv(self.path, 'all')


def production_main(
        path_gens,
        function_to_call_with_job,
        out_dir
):
    makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))

    with RunStatus(join(out_dir, 'runinfo.csv')) as runstatus:

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

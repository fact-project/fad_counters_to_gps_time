import fad_counters_to_gps_time as fad2gps
import photon_stream as ps
import tempfile
import os
from os.path import join
from os.path import exists
import pkg_resources


def test_production_idsc_qsub_production():
    with tempfile.TemporaryDirectory(prefix='fad2gps_') as tmp:

        runinfo_path = pkg_resources.resource_filename(
            'fad_counters_to_gps_time',
            'tests/resources/runinfo_2014Dec15_2015Jan15.msg')

        runinfo = ps.production.runinfo.read_runinfo_from_file(runinfo_path)

        fact_dir = join(tmp, 'fact')
        ps.production.runinfo.create_fake_fact_dir(fact_dir, runinfo)

        out_dir = join(tmp, 'out')

        fad2gps.production.isdc.qsub(
            out_dir=out_dir,
            run_info=runinfo,
            only_a_fraction=1.0,
            fact_raw_dir=join(fact_dir, 'raw'),
            use_dummy_qsub=True,
        )

        assert exists(join(tmp, 'out'))
        assert exists(join(tmp, 'out', 'fad'))
        assert exists(join(tmp, 'out', 'std'))
        assert exists(join(tmp, 'out', 'job'))

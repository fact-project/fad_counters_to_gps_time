import numpy as np
import fad_counters_to_gps_time as fad2gps
import tempfile
import os
from os.path import join
from os.path import exists
import pkg_resources
import glob


def test_production_write_worker_script():
    with tempfile.TemporaryDirectory(prefix='fad2gps_production') as tmp:
        worker_script_path = join(tmp, 'worker.sh')
        fad2gps.production.isdc.write_worker_node_script(
        	path=worker_script_path,
		    input_run_path='hans',
		    output_fad_counter_path='peter',
        )
        assert os.path.exists(worker_script_path)
        assert os.access(worker_script_path, os.X_OK)

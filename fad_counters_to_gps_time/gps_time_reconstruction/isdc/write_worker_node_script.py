import os
import stat


def write_worker_node_script(
    path,
    fad_counter_path,
    gps_time_path,
    models_path,
):
    """
    Writes an executable bash script for a worker node to extract the fad
    counters from one fact raw run.
    """
    sh = (
        "#!/bin/bash\n"
        "source $HOME/.bashrc\n"
        "var = 'gps_time_reconstruction {fad_counter_path} {gps_time_path}"
        " {models_path}'\n"
        "echo $var\n"
        "eval $var\n"
        ).format(
        fad_counter_path=fad_counter_path,
        gps_time_path=gps_time_path,
        models_path=models_path)
    with open(path, 'w') as fout:
        fout.write(sh)

    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

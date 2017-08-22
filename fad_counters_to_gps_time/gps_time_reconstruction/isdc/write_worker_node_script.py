import os, stat

def write_worker_node_script(
    path,
    input_run_path,
    output_fad_counter_path
):
    """
    Writes an executable bash script for a worker node to extract the fad 
    counters from one fact raw run.
    """
    sh = '#!/bin/bash\n'
    sh+= 'source $HOME/.bashrc\n'
    sh+= 'eval "fad_counter_extraction -i '+input_run_path+' -o '+output_fad_counter_path+'"\n'

    with open(path, 'w') as fout:
        fout.write(sh)

    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

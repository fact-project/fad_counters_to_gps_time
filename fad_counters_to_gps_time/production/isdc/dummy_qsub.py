import os
import numpy as np
import json
import subprocess
from os.path import exists
from os.path import split

def dummy_qsub(command):
    """
    Simulates a qsub service to enable unit testing of a qsub submitter.
    Asserts that the qsub parameters are valid and creates a dummy output 
    based on the input.

    Parameters
    ----------
    command         A qsub command list as it would be given to 
                    subprocess.call() in order to submitt to qsub.
    """
    assert command[0] == 'qsub'
    assert command[1] == '-q'
    queue = command[2]
    assert command[3] == '-o'
    stdout_path = command[4]
    assert command[5] == '-e'
    stderr_path = command[6]
    assert command[7] == '-m'
    assert command[8] == 'ae'
    assert command[9] == '-M'
    email = command[10]
    job_path = command[11]
    assert exists(split(stdout_path)[0])

    with open(stdout_path, 'w') as stdout:
        stdout.write('Dummy qsub:\n')
        stdout.write('stdout path: '+stdout_path+'\n')
        stdout.write('stderr path: '+stderr_path+'\n')
        stdout.write('email: '+email+'\n')
        stdout.write('job path: '+job_path+'\n')

    with open(stderr_path, 'w') as stderr:
        pass

    out_path = extract_out_path_from_worker_job(job_path)

    with open(out_path, 'wt') as out:
        out.write('{"comment": "I am a dummy output fad counter file"}\n')


def extract_out_path_from_worker_job(job_path):
    with open(job_path, 'r') as job:
        out_path = ''
        for line in job:
            at = line.find('-o')
            if at > 0:
                out_path = line[(at+2):-2].strip()
    return out_path
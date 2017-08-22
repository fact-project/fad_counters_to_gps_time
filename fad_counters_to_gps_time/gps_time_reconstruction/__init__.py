"""
Usage:
    gps_time_reconstruction <fad_counter_path> <gps_time_path> <meta_path>
"""
import docopt
from . import isdc
from . import ethz

import shutil


def gps_time_reconstruction(path):
    pass


def main(fad_counter_path, gps_time_path, meta_path):
    gps_time, meta = gps_time_reconstruction(fad_counter_path)

    gps_time.to_hdf(gps_time_path+'.part', 'all')
    shutil.move(gps_time_path+'.part', gps_time_path)

    meta.to_hdf(meta_path+'.part', 'all')
    shutil.move(meta_path+'.part', meta_path)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(
        fad_counter_path=args['<fad_counter_path>'],
        gps_time_path=args['<gps_time_path>'],
        meta_path=args['<meta_path>'],
        )

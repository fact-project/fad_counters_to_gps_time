import numpy as np
import pandas as pd
import single_gps_time_reco as tr
import pkg_resources

fad_counters_file_path = pkg_resources.resource_filename(
    'single_gps_time_reco',
    'tests/resources/20161231_109_fad.h5')


def test_get_gps():
    df = pd.read_hdf(fad_counters_file_path)
    gps_set = tr.get_gps(df)
    assert len(gps_set) == 288


def test_make_counter_increasing_do_nothing_when_counter_is_good_already():
    df = pd.read_hdf(fad_counters_file_path)
    df = tr.get_gps(df)
    # this counter happens to be okay.
    counter_1 = df['Counter_0']
    counter_2 = tr.make_counter_strictly_increasing(counter_1)

    assert (counter_2 == counter_1).all()


def test_make_counter_increasing_2():
    df = pd.read_hdf(fad_counters_file_path)
    df = tr.get_gps(df)
    counter_1 = df['Counter_0']

    # make a counter with a 32bit overflow roughly in the middle
    counter_max = 2**32
    counter_1 = pd.Series(
        index=counter_1.index,
        data=(np.arange(288)*int(1e4) + counter_max - int(144*1e4)).astype(np.uint32)
    )
    w = np.where(counter_1.astype(float).diff()[1:] <= 0)[0]
    assert len(w) == 1
    assert w[0] == 143

    # repair the overflow ...
    counter_2 = tr.make_counter_strictly_increasing(counter_1)

    # and make sure the new counter is really strictly increasing
    w = np.where(counter_2.astype(float).diff()[1:] <= 0)[0]
    assert len(w) == 0

    # but make also sure, that counter_2 and counter_1 are totally identical,
    # when both as cast to np.uint32
    assert ((counter_2.astype(np.uint32) - counter_1.astype(np.uint32)) == 0).all()

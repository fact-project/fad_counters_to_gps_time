import numpy as np
import pandas as pd
import single_gps_time_reco as tr


def test_date_range():
    '''
    make sure a `pd.date_range` is really just the array of integers, we expect it to be.
    We need this knowledge for the test below...
    '''
    start_time_str = "2015-01-01"
    start_time = pd.Timestamp(start_time_str)
    N_elements = 300
    date_range = pd.date_range(start_time_str, periods=N_elements, freq='s')
    step_1_second_in_ns = int(1e9)

    array_of_ints = np.arange(N_elements, dtype=np.int64) * step_1_second_in_ns
    array_of_ints += start_time.value

    for i in range(N_elements):
        assert array_of_ints[i] == date_range.values.astype(np.int64)[i]


def test_polyfit_1deg_for_big_ints():
    start_time_str = "2015-01-01"
    start_time = pd.Timestamp(start_time_str)
    N_elements = 300
    x_times = pd.date_range(start_time_str, periods=N_elements, freq='s')
    x_ns_integers = x_times.values.astype(np.int64)
    x_big_number = start_time.value

    y_big_number = int(2**31)
    y_counters = np.arange(300, dtype=np.uint64) * int(1e4) + y_big_number
    '''
    a = np.arange(300)
    x = x_big_number + a * 1e9
    y = y_big_number + a * 1e4

    a = (x - x_big_number) / 1e9
    y = y_big_number + (x - x_big_number) * 1e4/ 1e9
    y = (y_big_number - x_big_number * 1e-5) + x * 1e-5
    '''
    alpha = y_big_number - x_big_number * 1e-5
    beta = 1e-5

    fit_big_ints = np.polyfit(x_ns_integers, y_counters, deg=1)
    fit_smaller_ints = tr.more_precise_linear_fit(x_ns_integers, y_counters)

    assert np.abs((fit_smaller_ints[0] - beta) / beta) < 1e-15
    assert np.abs((fit_smaller_ints[1] - alpha) / alpha) < 1e-15

    assert np.abs((fit_big_ints[0] - beta) / beta) < 1e-9
    assert np.abs((fit_big_ints[1] - alpha) / alpha) < 1e-9

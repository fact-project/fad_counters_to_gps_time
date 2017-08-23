import time
import pandas as pd
import numpy as np


def test_time_time_to_pd_datetime():
    timestamp = time.time()
    timestamp_in_ns = int(timestamp * 1e9)
    pd_datetime = pd.to_datetime(timestamp_in_ns)
    assert pd_datetime.value == timestamp_in_ns


def test_pd_datetime_subtract():
    timestamp_in_ns = int(time.time() * 1e9)
    timestamp_in_ns_5ns_later = timestamp_in_ns + 5

    pd_datetime = pd.to_datetime(timestamp_in_ns)
    pd_datetime_5ns_later = pd.to_datetime(timestamp_in_ns_5ns_later)
    time_delta = pd_datetime_5ns_later - pd_datetime
    assert time_delta.value == 5


def test_number_of_bits_smaller_64_future():
    now = pd.Timestamp(time.time(), unit='s')
    delta_100_years = pd.Timedelta(weeks=5200)
    in_100_years = now + delta_100_years
    bits_needed = np.log(in_100_years.value)/np.log(2)
    assert bits_needed < 64


def test_number_of_bits_smaller_64_past():
    now = pd.Timestamp(time.time(), unit='s')
    delta_100_years = pd.Timedelta(weeks=5200)
    _100_years_ago = now - delta_100_years

    assert _100_years_ago.value < 2
    bits_needed = np.log(_100_years_ago.value * -1)/np.log(2)
    assert bits_needed < 64

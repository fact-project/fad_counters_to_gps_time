#!/bin/env python
"""
Usage:
    gps_time_reconstruction <fad_counter_path> <gps_time_path> <models_path>
"""
from os.path import abspath
from os.path import dirname
from os import makedirs
import shutil
import docopt
import pandas as pd
import numpy as np
from scipy.stats import chi2
from fact.instrument import trigger

SQUARE_TIME_ERROR_OF_COUNTER = (1e-8/12) * 1e9 * 1e9 # in ns
MAX_RESIDUAL_MEAN = 5e3 # in ns
MAX_RESIDUAL_STD = np.sqrt(1e-5) * 1e9  # in ns
MIN_P_VALUE = 1e-4
COUNTER_MAX = 2**32


class TooFewGpsEvents(Exception):
    '''Raised when input file has not enough GPS events'''
    pass


def main():
    args = docopt.docopt(__doc__)
    args['<fad_counter_path>'] = abspath(args['<fad_counter_path>'])
    args['<gps_time_path>'] = abspath(args['<gps_time_path>'])
    args['<models_path>'] = abspath(args['<models_path>'])

    write_gps_time_reconstruction(
        fad_counter_path=args['<fad_counter_path>'],
        gps_time_path=args['<gps_time_path>'],
        models_path=args['<models_path>'],
        )


def write_gps_time_reconstruction(
        fad_counter_path,
        gps_time_path,
        models_path
):
    gps_time, models = gps_time_reconstruction(fad_counter_path)

    makedirs(dirname(gps_time_path), exist_ok=True)
    gps_time.to_hdf(gps_time_path+'.part', 'all')
    shutil.move(gps_time_path+'.part', gps_time_path)

    makedirs(dirname(models_path), exist_ok=True)
    models.to_hdf(models_path+'.part', 'all')
    shutil.move(models_path+'.part', models_path)


def gps_time_reconstruction(path):
    df = pd.read_hdf(path)
    df['time'] = df.UnixTime_ns / 1e9
    df['time_rounded'] = df.time.round() * 1e9
    for board_id in range(40):
        df['Counter_{0:02d}'.format(board_id)] = make_counter_strictly_increasing(
            df['Counter_{0:02d}'.format(board_id)])

    gps_set = get_gps(df)
    if len(gps_set) < 10:
        raise TooFewGpsEvents
    training_set = gps_set.sample(frac=2/3)
    models = train_models(training_set)
    df['GpsTime'] = apply_models(models, df).round().astype(np.uint64)

    test_set = get_gps(df).drop(training_set.index)
    residuals = test_set.GpsTime - test_set.time_rounded

    models['fNight'] = df.fNight.iloc[0]
    models['fRunID'] = df.fRunID.iloc[0]
    models['fRunStart'] = pd.to_datetime(df.time.mean(), unit='s')
    
    models['residuals_mean'] = residuals.mean()
    models['residuals_std'] = residuals.std()
    models['is_residual_mean_good'] = residuals.mean() < MAX_RESIDUAL_MEAN
    models['is_residual_std_good'] = residuals.std() < MAX_RESIDUAL_STD
    models['is_p_values_good'] = (models.p_value > MIN_P_VALUE).all()
    models['is_tooth_gaps_good'] = (pd.to_datetime(gps_set.time.round(), unit='s').dt.second != 0).all()

    out_df = df[['fNight', 'fRunID', 'Event', 'Trigger', 'UnixTime_ns', 'GpsTime']]
    return out_df, models


def make_counter_strictly_increasing(counter):
    counter = counter.astype(np.int64)
    w = np.where(counter.diff() < 0)[0]
    if not w:
        return counter
    else:
        v = counter.values
        v[w[0]:] += COUNTER_MAX
        counter = pd.Series(index=counter.index, data=v)
        return counter


def get_gps(df):
    return df[df.Trigger.isin([trigger.EXT1, trigger.EXT2])]


def train_models(df):
    fits = []
    for board_id in range(40):
        X = df['Counter_{0:02d}'.format(board_id)].values
        Y = df.time_rounded.values

        fit = more_precise_linear_fit(X, Y)
        residuals = Y - np.polyval(fit, X)
        _chi2 = (residuals**2).sum() / SQUARE_TIME_ERROR_OF_COUNTER
        p_value = chi2.sf(_chi2, len(df) - 2)
        slope, intercept = fit
        fits.append({
            'board_id': board_id,
            'slope': np.float128(slope),
            'intercept': np.float128(intercept),
            'p_value': p_value,
            'chi2': _chi2,
            })
    return pd.DataFrame(fits)


def more_precise_linear_fit(X, Y):
    fit = np.polyfit(X - X[0], Y - Y[0], deg=1)
    fit = (fit[0], (fit[1] + Y[0] - fit[0]*X[0]))
    return fit


def apply_models(models, df):
    prediction = np.zeros((len(models), len(df)), dtype=np.float128)
    for m in models.itertuples():
        counter = df['Counter_{0:02d}'.format(m.board_id)]
        prediction[m.board_id, :] = m.intercept + m.slope * counter
    return prediction.mean(axis=0)


if __name__ == '__main__':
    main()

#!/bin/env python
"""
Usage:
    gps_time_reconstruction <fad_counter_path> <gps_time_path> <models_path>
"""
import docopt
import pandas as pd
import numpy as np
from scipy.stats import chi2
import shutil
from fact.instrument import trigger
from . import isdc
import os.path

SQUARE_TIME_ERROR_OF_COUNTER = 1e-8/12
MAX_RESIDUAL_MEAN = 5e-6
MAX_RESIDUAL_STD = np.sqrt(1e-5)
MIN_P_VALUE = 1e-4


def get_gps(df):
    return df[df.Trigger.isin([trigger.EXT1, trigger.EXT2])]


def train_models(df):
    fits = []
    for board_id in range(40):
        X = df['Counter_{0}'.format(board_id)]
        Y = df.time_rounded
        fit = np.polyfit(X, Y, deg=1)
        residuals = Y - np.polyval(fit, X)
        _chi2 = (residuals**2).sum() / SQUARE_TIME_ERROR_OF_COUNTER
        p_value = chi2.sf(_chi2, len(df) - 2)
        slope, intercept = fit
        fits.append({
            'board_id': board_id,
            'slope': slope,
            'intercept': intercept,
            'p_value': p_value,
            'chi2': _chi2,
            })
    return pd.DataFrame(fits)


def apply_models(models, df):
    prediction = np.zeros((len(models), len(df)), dtype='f8')
    for m in models.itertuples():
        counter = df['Counter_{0}'.format(m.board_id)]
        prediction[m.board_id, :] = m.intercept + m.slope * counter
    return prediction.mean(axis=0)


def gps_time_reconstruction(
        path,
        ):
    df = pd.read_hdf(path)
    df['time'] = pd.to_datetime(df.UnixTime, unit='s')
    df['time_rounded'] = df.UnixTime.round()
    df['time_diff'] = df.time_rounded - df.UnixTime

    gps_set = get_gps(df)
    training_set = gps_set.sample(frac=2/3)
    models = train_models(training_set)
    df['GpsTime'] = apply_models(models, df)

    test_set = get_gps(df).drop(training_set.index)
    residuals = test_set.GpsTime - test_set.time_rounded

    models['Night'] = df.Night.iloc[0]
    models['Run'] = df.Run.iloc[0]
    models['fRunStart'] = pd.to_datetime(df.UnixTime.mean(), unit='s')

    models['is_residual_mean_good'] = residuals.mean() < MAX_RESIDUAL_MEAN
    models['is_residual_std_good'] = residuals.std() < MAX_RESIDUAL_STD
    models['is_p_values_good'] = (models.p_value > MIN_P_VALUE).all()
    models['is_tooth_gaps_good'] = (gps_set.time.dt.second != 0).all()

    out_df = df[['Night', 'Run', 'Event', 'Trigger', 'UnixTime', 'GpsTime']]
    return out_df, models


def write_gps_time_reconstruction(fad_counter_path, gps_time_path, models_path):
    gps_time, models = gps_time_reconstruction(fad_counter_path)

    os.makedirs(os.path.split(gps_time_path)[0], exist_ok=True)
    gps_time.to_hdf(gps_time_path+'.part', 'all')
    shutil.move(gps_time_path+'.part', gps_time_path)

    os.makedirs(os.path.split(models_path)[0], exist_ok=True)
    models.to_hdf(models_path+'.part', 'all')
    shutil.move(models_path+'.part', models_path)


def main():
    args = docopt.docopt(__doc__)
    write_gps_time_reconstruction(
        fad_counter_path=args['<fad_counter_path>'],
        gps_time_path=args['<gps_time_path>'],
        models_path=args['<models_path>'],
        )


if __name__ == '__main__':
    main()

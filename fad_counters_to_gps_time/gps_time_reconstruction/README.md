# FACT FAD counters to GPS time

from: https://github.com/fact-project/fad_counters_to_gps_time

## Start Production at ISDC:

Have a look at the help to see how it works:

    neise@isdc-in04:~$ produce_gps_time_at_isdc --help
    Usage:
        isdc_production [options]

    Options:
        -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
        -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
        -f, --fraction F  fraction of possible jobs to submit [default: 1.0]

The input and output default paths, are here mainly for documentary purposes.
Changing the input path, will most probably not help you at all, but of course
you can decide to write the output somewhere else.

The `--fraction` parameter is also mainly there for testing purposes.

At the moment `produce_gps_time_at_isdc` is quite verbose.
Have a look at an example output.

Process accurate, GPS based time stamps for all FACT events using the GPS trigger and the 40 FAD counters of the FACT camera.

This is the Sub-README from [here](https://github.com/fact-project/fad_counters_to_gps_time/tree/gps_time_reco/fad_counters_to_gps_time/gps_time_reconstruction).

Since you are looking at this README, I assume you just found the output folder of
this part of the project. I nothing changed, you might find this README in:

    isdc:/gpfs0/fact/processing/gps_time

Let me explain the 3 subfolders you find here. In the 3 subfolders you'll find

    1) gps_time:
        * gps_time/yyyy/mm/dd/yyyymmdd_rrr_gps_time.h5
    2) gps_time_models:
        * gps_time_models/yyyy/mm/dd/yyyymmdd_rrr_models.h5
    3) std
        * std/yyyy/mm/dd/yyyymmdd_rrr.o
        * std/yyyy/mm/dd/yyyymmdd_rrr.e

So in `std/` there's only stdout and stderr, which should be not interesting.
In `gps_time_models/` you can find the models, which were trained in the process
in order to predict the GPS time for each event in the raw data.

And finally in `gps_time`, you'll find relatively small h5 files, containing dataframes like this:

```
In [1]: import pandas as pd
In [2]: df = pd.read_hdf('gps_time/yyyy/mm/dd/yyyymmdd_rrr_gps_time.h5')
In [3]: len(df)
Out[3]: 17792

In [4]: df.head()
Out[4]:
      Night  Run  Event  Trigger      UnixTime       GpsTime
0  20160630   62      1        4  1.467328e+09  1.467328e+09
1  20160630   62      2        4  1.467328e+09  1.467328e+09
2  20160630   62      3        4  1.467328e+09  1.467328e+09
3  20160630   62      4        4  1.467328e+09  1.467328e+09
4  20160630   62      5        4  1.467328e+09  1.467328e+09

In [5]: df.dtypes
Out[5]:
Night        uint32
Run          uint32
Event        uint32
Trigger      uint16
UnixTime    float64
GpsTime     float64
dtype: object
```

You see, both timestamps `UnixTime` and `GpsTime` are stored as float64 types.
The numbers stand for seconds since 01.01.1970, so they are unix timestamps,
in UTC. Including all the problems, like leap seconds and so on.


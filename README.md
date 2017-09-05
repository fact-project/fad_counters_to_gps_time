# FAD counters to GPS time

The FACT events are created by the event-builder program which assigns each event a time stamp based on the system time of the machine where the event-builder is running. This time stamp is only accurate in the ~1ms regime, e.g. due to ethernet network delays. However, for studies which need a high timing precision like pulsar studies, a more precise time stamp is needed.

## crontab at ISDC

This is how I set up the continous gps time production at ISDC.
Redirecting the output of the production scripts into log files is not scrictly
necessary, but the cron at ISDC is not sending me any emails, so I'd like to
look at the logs every now and then...

    11    12    * * *  source $HOME/.bashrc; fad_counter_production &> fad_counter_production.log
    21    12    * * *  source $HOME/.bashrc; gps_time_production &> gps_time_production.log

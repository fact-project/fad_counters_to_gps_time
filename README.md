# FAD counters to GPS time

The FACT events are created by the event-builder program which assigns each event a time stamp based on the system time of the machine where the event-builder is running. This time stamp is only accurate in the ~1ms regime, e.g. due to ethernet network delays. However, for studies which need a high timing precision like pulsar studies, a more precise time stamp is needed.  

# FACT FAD counters to GPS time

https://github.com/fact-project/fad_counters_to_gps_time

Process accurate, GPS based time stamps for all FACT events using the GPS trigger and the 40 FAD counters of the FACT camera.

## ./fad

Contains the FAD counters for all events extracted from the FACT raw files. The files are stored run wise:

```bash
./fad/yyyy/mm/nn/yyyymmnn_rrr_fad.h5
```

## ./std

Contains the stdout and stderror of the fad counter extraction. Might be different depending on what infrastructure the processing was done.

## ./job

Contains the job scripts of the isdc-qsub-based fad counter extraction (not all runs have this).
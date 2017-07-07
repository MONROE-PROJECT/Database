#!/bin/bash

for fichero in 206_292/*.csv; do  ./importTrainTelemetry.py -n 206 -f $fichero; done
for fichero in 206_292/*.csv; do  ./importTrainTelemetry.py -n 292 -f $fichero; done

for fichero in 228_229/*.csv; do  ./importTrainTelemetry.py -n 228 -f $fichero; done
for fichero in 228_229/*.csv; do  ./importTrainTelemetry.py -n 229 -f $fichero; done

for fichero in 254_255/*.csv; do  ./importTrainTelemetry.py -n 254 -f $fichero; done
for fichero in 254_255/*.csv; do  ./importTrainTelemetry.py -n 255 -f $fichero; done

for fichero in 261_291/*.csv; do  ./importTrainTelemetry.py -n 261 -f $fichero; done
for fichero in 261_291/*.csv; do  ./importTrainTelemetry.py -n 291 -f $fichero; done

for fichero in 289_290/*.csv; do  ./importTrainTelemetry.py -n 289 -f $fichero; done
for fichero in 289_290/*.csv; do  ./importTrainTelemetry.py -n 290 -f $fichero; done

for fichero in 296_297/*.csv; do  ./importTrainTelemetry.py -n 296 -f $fichero; done
for fichero in 296_297/*.csv; do  ./importTrainTelemetry.py -n 297 -f $fichero; done

for fichero in 304_305/*.csv; do  ./importTrainTelemetry.py -n 304 -f $fichero; done
for fichero in 304_305/*.csv; do  ./importTrainTelemetry.py -n 305 -f $fichero; done


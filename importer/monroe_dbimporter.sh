#!/bin/bash
export MONROE_DB_USER=USER
export MONROE_DB_PASSWD=SECRETPASSWORD 

for path in "http" "metadata" "mplane" "ping" "retry"
do
        if [[ ! $(screen -r |grep MonroeDB-${path}) ]]
        then
                if [[ ${path} == "http" ]] 
                then
                        echo "Starting a recursive Importer for ${path}"
                        screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/monroe/${path} --failed /experiments/failed/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=1 --concurren$
                elif [[ ${path} == "retry" ]] 
                then
                        echo "Starting a recursive Importer for ${path} (extensive logging)"
                        screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/monroe/${path} --failed /experiments/failed/retry/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=2 --con$
                else
                        echo "Starting a non recursive Importer for ${path}"
                        screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/monroe/${path} --failed /experiments/failed/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=1 --concurren$
                fi
        fi
done
screen -r |grep MonroeDB-

#!/bin/bash
export MONROE_DB_USER=<USER>
export MONROE_DB_PASSWD=<PASSWORD>

mkdir -p /experiments/processed/
mkdir -p /experiments/failed-retries/
mkdir -p /experiments/failed/


for path in "http" "metadata" "mplane" "ping" "retry" "traceroute"
do
	if [[ ! $(screen -r |grep MonroeDB-${path}) ]]
	then
		mkdir -p /experiments/monroe/${path}
		if [[ ${path} == "http" ]]  || [[ ${path} == "traceroute" ]]
		then
			echo "Starting a recursive Importer for ${path}"
			screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/monroe/${path} --failed /experiments/failed/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=1 --concurrency=2 --recursive | tee importer_${path}.log"
		elif [[ ${path} == "retry" ]]
		then
			echo "Starting a recursive Importer for ${path} (extensive logging)"
			screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/failed/ --failed /experiments/failed-retries/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=2 --concurrency=1 --recursive | tee importer_${path}.log"
		else
			echo "Starting a non recursive Importer for ${path}"
			screen -dmS MonroeDB-${path} bash -c "python -u monroe_dbimporter.py --indir /experiments/monroe/${path} --failed /experiments/failed/ --processed /experiments/processed/ --authenv  --host=127.0.0.1 --keyspace=monroe --interval=30  --verbosity=1 --concurrency=2 |tee importer_${path}.log"
		fi
	fi
done
screen -r |grep MonroeDB-

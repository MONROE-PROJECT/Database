#!/bin/bash
# This script compresses all the JSON files in the failed and processed folders that do not correspond to the current date.
# It should be run from cron after midnight.

shopt -s nullglob

backupPath=/experiments/backups
logPath=/var/log/autocopy.log
srcPath=/experiments
#backupPath=/home/mikepeon/experiments/backups
#logPath=/home/mikepeon/var/log/autocopy.log
#srcPath=/home/mikepeon/autocopy

exec 1<&-
exec 2<&-
exec 1>>$logPath
exec 2>&1

today=`date -I`
echo ----------------------------------
echo ----------------------------------
echo ----------------------------------
echo Autocopy running at `date -I'seconds'`

function DoFolder {
	cd ${srcPath}/${1}
	for folder in *; do
		if [ "${folder}" != "${today}" ]; then
			destFile=${backupPath}/${1}-${folder}.txz
			echo Processing $1 folder ${folder} into ${destFile}
			tar cJf ${destFile} ${folder}/
			if (($? != 0)); then
				echo Error creating file ${destFile}
				continue
			fi;
			xz -t -q ${destFile} 
			if (($? != 0)); then
				echo Error testing file ${destFile}
				continue
			fi;
			rm -rf ${folder}
			if (($? != 0)); then
				echo Error deleting folder ${folder}
				continue
			fi;
			echo File created and folder removed.
		else
			echo Ignorando $1 folder $folder;
		fi;
	done
}

DoFolder failed
DoFolder processed


#!/bin/bash

# SET DEFAULT VARIABLES
HOST=$HOSTNAME
SHORT_DATE=`date '+%Y-%m-%d'`
TIME=`date '+%H%M'`
SCRIPT_TYPE=`basename $0 | cut -d '.' -f1` 

filenametime1=$(date +"%m%d%Y%H%M%S")

# SET VARIABLES 
export PYTHON_SCRIPT_NAME=$(cat config.toml | grep 'py_script' | awk -F"=" '{print $2}' | tr -d '"') 
export SCRIPTS_FOLDER=$(pwd)
export LOGDIR=$SCRIPTS_FOLDER/log
export LOG_FILE=${LOGDIR}/${filenametime1}.log

# SET LOG RULES
exec > >(tee ${LOG_FILE}) 2>&1

# RUN SCRIPT
source venv/bin/activate
echo "Start to run Python Script"
python3 ${PYTHON_SCRIPT_NAME}


RC1=$?
if [ ${RC1} != 0 ]; then
	echo "PYTHON RUNNING FAILED"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
	exit 1
fi

echo "PROGRAM SUCCEEDED"

deactivate

exit 0 
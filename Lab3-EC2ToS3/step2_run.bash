#!/bin/bash

# Part 1: SET DEFAUL VARIABLES
HOST=$HOSTNAME
filenametime=$(date +"%Y%m%d-%H:%M:%S")

# Part 2: SET VARIABLES 
export PYTHON_SCRIPT_NAME=$(cat config.toml | grep 'py_script' | awk -F"=" '{print $2}' | tr -d '"') # get py_script value from config.toml
export SCRIPTS_FOLDER=$(pwd)
export LOGDIR=$SCRIPTS_FOLDER/log
if [ ! -d $LOGDIR ]
then
    echo "$LOGDIR not existed, now created"
    mkdir -p $LOGDIR
fi
export LOG_FILE=${LOGDIR}/${filenametime}.log

# PART 3: SET LOG RULES
exec > >(tee ${LOG_FILE}) 2>&1

# PART 4: RUN SCRIPT
source Lab3venv/bin/activate

echo "Start to run Python Script"
python3 ${SCRIPTS_FOLDER}/${PYTHON_SCRIPT_NAME}


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
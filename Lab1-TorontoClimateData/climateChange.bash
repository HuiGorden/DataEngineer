#!/bin/bash
#shopt -s xpg_echo

# Setup log file
TIME_STRING=$(date '+%Y-%m-%d_%H-%M-%S')
WORKING_DIR="/Users/huijin/Desktop/DataEngineeringLearning/Lab1-TorontoClimateData/${TIME_STRING}"
LOG_DIR="${WORKING_DIR}/logs"
LOG_FILE="${LOG_DIR}/running.log"
CSVFILES_DIR="${WORKING_DIR}/csv_files/"
OUTPUT_DIR="${WORKING_DIR}/output_files/"

# Create dirs
echo "Create working dir"
`mkdir ${WORKING_DIR}`
echo "Create log dir"
`mkdir ${LOG_DIR}`
echo "Create csv dir"
`mkdir ${CSVFILES_DIR}`
echo "Create output dir"
`mkdir ${OUTPUT_DIR}`

# Redirect log content to file
exec > >(tee ${LOG_FILE}) 2>&1

# Download csv files
echo "Start download data.."

for year in {2020..2022}
do curl "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=48549&Year=${year}&Month=2&timeframe=1&submit=Download+Data" -o ${CSVFILES_DIR}/${year}.csv;
done;

RC1=$?
if [ ${RC1} != 0 ]
then
	echo "DOWNLOAD DATA FAILED"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	exit 1
fi
echo "download csv file done."

# Call Python Script
echo "Start to run Python Script"
`python3 ${WORKING_DIR}/../dataConcatenate.py ${CSVFILES_DIR} ${OUTPUT_DIR}`


RC1=$?
if [ ${RC1} != 0 ]
then
	echo "PYTHON RUNNING FAILED"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	exit 1
fi

echo "PROGRAM SUCCEEDED"

exit 0 
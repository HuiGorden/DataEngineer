#!/bin/bash

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate 

# Install dependencies
pip install -r requirements.txt

# deactivate virtual environment
deactivate 

# make run.sh executable
chmod a+x run.bash

# create log directory if it doesn't exist
mkdir -p log 
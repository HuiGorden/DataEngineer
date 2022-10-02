#!/bin/bash

# install awscli
# sudo apt install awscli -y

# Create a python virtual environment 
sudo apt install python3.10-venv -y

python3 -m venv Lab3venv
if [ $? -eq 0 ]
then
    source Lab3venv/bin/activate 
    # Install dependencies
    pip install -r requirements.txt
    deactivate # deactivate python virtual environment
    echo "Initializtion Finish!"
else
    echo "Create python virtual environment failed!"
fi
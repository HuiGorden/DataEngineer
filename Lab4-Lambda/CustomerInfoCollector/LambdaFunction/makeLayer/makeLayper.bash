#!/bin/bash
mkdir -p python/lib/python3.8/site-packages
if [ $? -eq 0 ]
then
pip3 install -r requirements.txt --target python/lib/python3.8/site-packages
zip -r9 lambda-layer.zip python/
fi
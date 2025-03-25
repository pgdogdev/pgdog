#!/bin/bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python read_parquet.py $1

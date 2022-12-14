#!/bin/bash

# set python path
export PYTHONPATH=/home/gruppotavola/gruppotavola/src

# launch google analytics api retrieval everyday at 6.10
cd /home/gruppotavola/gruppotavola/src/googleanalytics/
python3 ga_api.py

# launch google analytics data cleaning
cd /home/gruppotavola/gruppotavola/src/googleanalytics/
python3 ga_clean.py

# launch oracle data cleaning
cd /home/gruppotavola/gruppotavola/src/oracle/
python3 oracle_clean_employees.py

cd /home/gruppotavola/gruppotavola/src/oracle/
python3 oracle_clean_guests.py

# launch seven rooms email retrieval
cd /home/gruppotavola/gruppotavola/src/sevenrooms/
python3 sevenrooms.py

# launch sevenrooms future reservations data cleaning
cd /home/gruppotavola/gruppotavola/src/sevenrooms/
python3 sevenrooms_clean_future.py

# launch sevenrooms past reservations data cleaning
cd /home/gruppotavola/gruppotavola/src/sevenrooms/
python3 sevenrooms_clean_yesterday.py

# forecasting
cd /home/gruppotavola/gruppotavola/src/forecast/
python3 forecast.py

# silver ingestion
cd /home/gruppotavola/gruppotavola/src/silver/
python3 silver.py

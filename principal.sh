#!/bin/bash
ECHO "HOLA ACA INGRESO"
rm -r weather-dataset-rattle-package.zip 
rm -r weatherAUS.csv 
kaggle datasets download -d jsphyg/weather-dataset-rattle-package --force
unzip weather-dataset-rattle-package.zip
psql -h 130.211.118.80 -d postgres -U postgres -c "\copy weather_1 FROM '~/weatherAUS.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')" -W ml2password
python3 model.py
python3 connection.py

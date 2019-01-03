#!/bin/bash

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

app_path=/home/PerformanceAnalysis/Performance/AccountMonitor


cd $app_path
echo "Run daily Basic info import"
/usr/local/anaconda3/bin/python Daily_BPI_data_import_to_MySQL.py

echo "Run daily Advanced info import"
/usr/local/anaconda3/bin/python Daily_API_data_import_to_MySQL.py

echo "Run daily email generate and reporting by html"
/usr/local/anaconda3/bin/python Daily_Daily_Reporting_by_Email.py
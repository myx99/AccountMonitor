#!/bin/bash

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

app_path=/home/PerformanceAnalysis/Performance/AccountMonitor
py=/usr/local/anaconda3/bin/python

cd $app_path

# $py Others/test.py

$py Daily_BPI_data_import_to_MySQL.py

$py Daily_API_data_import_to_MySQL.py

$py Daily_ANRM_import_to_MySQL.py

$py Daily_Reporting_by_Email.py
#!/bin/bash

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

app_path=/home/PerformanceAnalysis/Performance/AccountMonitor


cd $app_path
/usr/local/anaconda3/bin/python Batch_BPI_data_import_to_MySQL.py
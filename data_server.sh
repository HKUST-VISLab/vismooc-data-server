#!/bin/bash

# This file should be put in the father directory of vismooc-data-server, 
# and config.json is a example config file
# This script should be added to linux cron

if test -z $1; then
	python ./vismooc-data-server/main.py > /dev/null 2>&1 &
else
   	config_file_abs=`realpath $1`
	echo $config_file_abs
	# python ./vismooc-data-server/main2.py $config_file_abs
	python ./vismooc-data-server/main.py $config_file_abs > /dev/null 2>&1 &
	# python ./vismooc-data-server/main.py $config_file_abs
fi

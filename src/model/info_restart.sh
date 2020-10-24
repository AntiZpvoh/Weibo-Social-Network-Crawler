#! /bin/bash

while true 
do
	monitor=`ps -ef | grep "python3.7 handler.py info_consumer" | grep -v grep | wc -l ` 
	if [ $monitor -eq 0 ] 
	then
		echo "Manipulator program is not running, restart Manipulator"
		python3.7 handler.py info_consumer &
	else
		echo "Manipulator program is running"
	fi
	sleep 5
done
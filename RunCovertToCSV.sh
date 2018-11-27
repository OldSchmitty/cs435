#!/bin/bash
# This script takes 0-3 arguments
# ./RunCovertToCSV.sh <Input folder> <Output folder> <Jar>
# If there is nothing specified default values are used

# Default values
inputDir='/projectRawData/'
outputDir='/projectDataCSV/'
jar='./target/sqlToCsv-1.0.jar'


table=$1
header=$2
reducers=$3



# Run all jobs at Once by having 2 of them run in the background
# This works but mixes the console outputs
nice -20 $HADOOP_HOME/bin/hadoop jar ${jar} DriverCSV ${inputDir}${table} ${outputDir}${table} ${header} ${reducers} &
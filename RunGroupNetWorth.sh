#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playersAccountInfoFile='/projectDataCSV/PlayerAccountData'
groupFiles='/projectDataCSV/Groups/'

# change links
if [ $# -gt 0 ]
  then
    playersAccountInfoFile=$1
fi

if [ $# -gt 1 ]
  then
    groupFiles=$2
fi

mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.dataproccessing.FindGroupNetWorth --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playersAccountInfoFile} ${groupFiles}

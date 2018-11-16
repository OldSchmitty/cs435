#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playersNetWorthFile='/projectDataCSV/PlayersNetWorth'
groupFiles='/projectDataCSV/Groups/Groups-r-00000'

# change links
if [ $# -gt 0 ]
  then
    playersNetWorthFile=$1
fi

if [ $# -gt 1 ]
  then
    groupFiles=$2
fi


nice -20 $SPARK_HOME/bin/spark-submit --class cs435.dataproccessing.FindGroupNetWorth --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playersNetWorthFile} ${groupFiles}

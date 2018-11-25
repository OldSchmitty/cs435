#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playerId='76561198039495541'
playerInfoFiles='/projectDataCSV/PlayersNetWorth/playerNetWorth.csv'

# change links
if [ $# -gt 0 ]
  then
    playerInfoFiles=$1
fi

if [ $# -gt 1 ]
  then
    playerId=$2
fi

mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.dataproccessing.FindGivenUser --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playerInfoFiles} ${playerId}

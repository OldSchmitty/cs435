#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playerAndGenreDir='/projectDataCSV/PlayerGamesAndGenres/'
groupPlayerDir='/projectDataCSV/GroupsPlayers/'
playerGroupsDir='/projectDataCSV/PlayersGroups/'




# change links
if [ $# -gt 0 ]
  then
    playerAndGenreDir=$1
fi

if [ $# -gt 1 ]
  then
    groupPlayerDir=$2
fi

if [ $# -gt 2 ]
  then
    playerGroupsDir=$3
fi



mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.dataproccessing.FindStatsOnArrays --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playerAndGenreDir} ${groupPlayerDir} ${playerGroupsDir}

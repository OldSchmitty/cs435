#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playerStatsDir='/projectDataCSV/PlayerGamesAndGenres'
playerWorthDir='/projectDataCSV/PlayerAccountData/'



# change links
if [ $# -gt 0 ]
  then
    playerStatsDir=$1
fi


mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.summarization.FindTopGenre --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playerStatsDir} ${playerWorthDir}
#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

playerGamesFiles='/projectDataCSV/Games/'
gameInfoFiles='/projectDataCSV/App_ID_Info/'
playerFiles='/projectDataCSV/Player_Summaries/'
gameGenreFiles='/projectDataCSV/Games_Genres/'



# change links
if [ $# -gt 0 ]
  then
    playerGamesFiles=$1
fi

if [ $# -gt 1 ]
  then
    gameInfoFiles=$2
fi

if [ $# -gt 2 ]
  then
    playerFiles=$3
fi

if [ $# -gt 3 ]
  then
    gameGenreFiles=$4
fi

mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.dataproccessing.FindNetWorth --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${playerGamesFiles} ${gameInfoFiles} ${playerFiles} ${gameGenreFiles}

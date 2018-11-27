#!/usr/bin/env bash

jar='./target/ShrimpsVsWhales-1.0.jar'

groupPlayerDir='/projectDataCSV/GroupPlayerStatistics'
userGroups='/projectDataCSV/UsersNumberOfGroups'




# change links
if [ $# -gt 0 ]
  then
    groupPlayerDir=$1
fi




mvn package
nice -20 $SPARK_HOME/bin/spark-submit --class cs435.regression.RegressionGroups --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${groupPlayerDir}

# nice -20 $SPARK_HOME/bin/spark-submit --class cs435.regression.RegressionUsers --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${userGroups}
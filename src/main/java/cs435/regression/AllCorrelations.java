package cs435.regression;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class AllCorrelations {


    public static void main(String[] args){
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Whales Vs Shrimp - Regression")
                    .master("local")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");
            SparkContext sc = spark.sparkContext();
            String GroupPlayerStatistics = args[0];
        String PlayerAccountData = args[1];

        Dataset GroupPlayerStatisticsDS = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(GroupPlayerStatistics);

        Dataset PlayerAccountDataDS = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(PlayerAccountData);


        GroupPlayerStatisticsDS.summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
                    "99%").show();


        GroupPlayerStatisticsDS.printSchema();

        GroupPlayerStatisticsDS = GroupPlayerStatisticsDS.na().drop();

            System.out.println("The correlation of NumberOfMembers and AverageMemberNetWorth  is: " + GroupPlayerStatisticsDS.stat().corr("NumberOfMembers","AverageMemberNetWorth"));
            System.out.println("The correlation of NumberOfMembers and AverageNumberOfGames is: " + GroupPlayerStatisticsDS.stat().corr("NumberOfMembers","AverageNumberOfGames"));
            System.out.println("The correlation of NumberOfMembers and AverageTotalPlayTime is: " + GroupPlayerStatisticsDS.stat().corr("NumberOfMembers","AverageTotalPlayTime"));


        PlayerAccountDataDS.na().drop().summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
                "99%").show();

        PlayerAccountDataDS.printSchema();

        //PlayerAccountDataDS = PlayerAccountDataDS.na().drop();

        System.out.println("The correlation of NumberOfGamesOwned and TotalPlayTime  is: " + PlayerAccountDataDS.stat().corr("NumberOfGamesOwned","TotalPlayTime"));
        System.out.println("The correlation of NetWorth and TotalPlayTime is: " + PlayerAccountDataDS.stat().corr("NetWorth","TotalPlayTime"));
        }
    }



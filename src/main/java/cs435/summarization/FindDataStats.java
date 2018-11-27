package cs435.summarization;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class FindDataStats {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("FindDataStats:: needs a path <PlayerGames> <GameInfo>");
    }

    String playerStatsDir = args[0];
    String gameInfoDir = args[1];
    String playerInfoDir = args[2];
    String playerNetWorthDir = args[3];

    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - Data Stats")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");


    Dataset playerInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerStatsDir);


    playerInfo.printSchema();

    playerInfo.summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
        "99%" ,"99.9%").show();

    playerInfo.agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth")).show();

    Dataset totals = playerInfo
        .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
        .withColumn("PercentOfUsers", lit(100.0))
        .withColumn("PercentOfNetWorth", lit(100.0))
        .withColumn("Percentile", lit("AllUsers")).cache();

    List<Row> totalsValueOfDataSet = totals.select("NumberOfUsers","TotalNetWorth").toJavaRDD().take(1);

    long totalNumberOfIDs = (long) totalsValueOfDataSet.get(0).get(0);
    double totalNetWorth = (double) totalsValueOfDataSet.get(0).get(1);

    System.out.println("The total number of Ids: " + totalNumberOfIDs + "\n The total net worth: " + totalNetWorth);
/*
    double[] percentileAmounts = new double[]{ 0.25, 0.5, 0.75, 0.9, 0.93, 0.95, 0.97, 0.99, 0.999};
    double[] playerNetWorthBreaks = playerInfo.stat()
        .approxQuantile("NetWorth", percentileAmounts, 0.00001);

    for(int i = 0; i < percentileAmounts.length; i++){
      Dataset bottomPercent = playerInfo.filter("NetWorth < " + playerNetWorthBreaks[i])
          .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
          .withColumn("PercentOfUsers", col("NumberOfUsers").divide(totalNumberOfIDs))
          .withColumn("PercentOfNetWorth", col("TotalNetWorth").divide(totalNetWorth))
          .withColumn("Percentile", lit("Bottom: " + (percentileAmounts[i] * 100)));
      Dataset topPercent = playerInfo.filter("NetWorth > " + playerNetWorthBreaks[i])
          .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
          .withColumn("PercentOfUsers", col("NumberOfUsers").divide(totalNumberOfIDs))
          .withColumn("PercentOfNetWorth", col("TotalNetWorth").divide(totalNetWorth))
          .withColumn("Percentile", lit("Top: " + ((1 - percentileAmounts[i]) * 100)));

      bottomPercent.show();
      topPercent.show();


    }*/

    double[] percentileAmountsForBar = new double[]{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    double[] playerNetWorthBreaksBar = playerInfo.stat()
        .approxQuantile("NetWorth", percentileAmountsForBar, 0.00001);

    playerInfo.filter("NetWorth BETWEEN 0 AND " + playerNetWorthBreaksBar[0])
        .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
        .withColumn("PercentOfUsers", col("NumberOfUsers").divide(totalNumberOfIDs))
        .withColumn("PercentOfNetWorth", col("TotalNetWorth").divide(totalNetWorth))
        .withColumn("Grouping", lit("10%"))
        .withColumn("DollarAmount", lit(playerNetWorthBreaksBar[0])).show();



    for(int i = 0; i < percentileAmountsForBar.length -1; i++){
      playerInfo.filter("NetWorth BETWEEN " + playerNetWorthBreaksBar[i] + " AND " + playerNetWorthBreaksBar[i+1])
          .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
          .withColumn("PercentOfUsers", col("NumberOfUsers").divide(totalNumberOfIDs))
          .withColumn("PercentOfNetWorth", col("TotalNetWorth").divide(totalNetWorth))
          .withColumn("Grouping", lit(percentileAmountsForBar[i] + " to " + percentileAmountsForBar[i+1]))
          .withColumn("DollarAmount", lit(playerNetWorthBreaksBar[i] + " to " + playerNetWorthBreaksBar[i+1])).show();


    }

    playerInfo.filter("NetWorth > " + playerNetWorthBreaksBar[8])
        .agg(count("steamid").alias("NumberOfUsers"), sum("NetWorth").alias("TotalNetWorth"))
        .withColumn("PercentOfUsers", col("NumberOfUsers").divide(totalNumberOfIDs))
        .withColumn("PercentOfNetWorth", col("TotalNetWorth").divide(totalNetWorth))
        .withColumn("Grouping", lit("90% - 100"))
        .withColumn("DollarAmount", lit(playerNetWorthBreaksBar[8])).show();





    //System.out.println(Arrays.toString(playerInfo.stat().approxQuantile("NetWorth", new double[] {0.01, 0.10, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999}, 0.00001)));

  }


}

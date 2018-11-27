package cs435.summarization;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindTopGenre {


  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      throw new IllegalArgumentException("FindTopGenre:: needs a path <PlayerGenre>");
    }

    String playerGenreDir = args[0];
    String playerNetWorthDir = args[1];




    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - Genre Stats")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    Dataset playerNetWorth = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerNetWorthDir).select("steamid", "NetWorth");


    Dataset playerInfo = spark.read().parquet(playerGenreDir);


    playerNetWorth = playerNetWorth.filter("NetWorth > 1172");

    playerInfo = playerInfo.drop("NumberOfGamesOwned" ,"Games");

    playerInfo.join(playerNetWorth, "steamid").drop("steamid", "NetWorth").summary().show();






  }


}

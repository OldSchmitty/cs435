package cs435.dataproccessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindGivenUser {

  public static void main(String[] args) throws Exception {

    String playerSummaryDir = args[0];
    String player = args[1];

    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - UserLookup")
        .getOrCreate();

    Dataset userSummaries = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerSummaryDir);

    userSummaries.filter("steamid == "+player).show();




  }

}

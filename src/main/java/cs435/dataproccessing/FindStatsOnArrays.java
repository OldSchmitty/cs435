package cs435.dataproccessing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindStatsOnArrays {

  public static void main(String[] args) throws Exception {

    if (args.length < 3) {
      throw new IllegalArgumentException(
          "FindStatsOnArrays:: needs a path <PlayerAndGenre> <GroupPlayer> <PlayerGroups>");
    }

    // Get file locations
    String playerAndGenreDir = args[0];
    String groupPlayerDir = args[1];
    String playerGroupsDir = args[2];

    // Start Spark job
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - Array Info")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");


    Dataset playerAndGenre = spark.read().parquet(playerAndGenreDir);

    Dataset groupPlayer = spark.read().parquet(groupPlayerDir);

    Dataset playerGroups = spark.read().parquet(playerGroupsDir);

    playerAndGenre.show();
    playerAndGenre.printSchema();

    groupPlayer.show();
    groupPlayer.printSchema();

    playerGroups.show();
    playerGroups.printSchema();


    groupPlayer.filter("groupid == 833").select("groupid", "Members")
        .withColumn("steamid", explode(col("Members")))
        .join(playerAndGenre.drop("Games"), "steamid").show();


  }

}

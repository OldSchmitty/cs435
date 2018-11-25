package cs435.dataproccessing;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class FindGroupNetWorth {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("FindGroupNetWorth:: needs a path <PlayerAccountInfo> <GroupInfo>");
    }

    // Get file locations
    String playerAccountInfoDir = args[0];
    String groupInfoDir = args[1];

    // Start Spark job
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - Group Info")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    // Read Files
    Dataset playerAccountInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerAccountInfoDir)
        .select("steamid", "NumberOfGamesOwned", "TotalPlayTime", "NetWorth");

    Dataset groupInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(groupInfoDir)
        .select("steamid", "groupid");

    // Check to see if data was read properly
    System.out.println("There are " + playerAccountInfo.count() + " player's with a calculated net worth in the file");
    playerAccountInfo.show(10);
    playerAccountInfo.printSchema();

    System.out.println("There are " + groupInfo.count() + " group items in the file");
    groupInfo.show(10);
    groupInfo.printSchema();


    // Join the group data and the net worth of all the members
    Dataset playerGroups = groupInfo.join(playerAccountInfo, "steamid");

    playerGroups.show(10);
    playerGroups.summary();

    // Find the groups total net worth and average users net worth
    Dataset<Row> groupNetWorth = playerGroups.groupBy("groupid")
        .agg(count("steamid").alias("NumberOfMembers"),
            avg("NetWorth").alias("AverageMemberNetWorth"),
            sum("NetWorth").alias("NetWorthOfGroup"),
            avg("NumberOfGamesOwned").alias("AverageNumberOfGames"),
            sum("NumberOfGamesOwned").alias("TotalNumberOfGames"),
            avg("TotalPlayTime").alias("AverageTotalPlayTime"),
            sum("TotalPlayTime").alias("TotalPlayTime"));

    groupNetWorth.orderBy(col("NumberOfMembers").desc()).show(30);
    groupNetWorth.orderBy(col("AverageMemberNetWorth").desc()).show(30);
    groupNetWorth.orderBy(col("NetWorthOfGroup").desc()).show(30);
    groupNetWorth.select("AverageMemberNetWorth", "NetWorthOfGroup", "NumberOfMembers").summary().show();




    groupNetWorth.coalesce(10).write()
        .mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
        .option("header", "true")
        .save("steamProject/GroupNetWorth");


    // Find how many groups a user is in

    Dataset<Row> userGroups = playerGroups.groupBy("steamid")
        .agg(count("groupid").alias("NumberOfGroups")).join(playerAccountInfo, "steamid");

    userGroups.show();


    userGroups.coalesce(10).write()
        .mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
        .option("header", "true")
        .save("steamProject/UsersNumberOfGroups");


    // Store what SteamId's belong to a group
    Dataset<Row> groupMembers = playerGroups.groupBy("groupid")
        .agg(collect_list("steamid").alias("Members"), count("steamid").alias("NumberOfMembers"));
    groupMembers.show();
    groupMembers.printSchema();


    groupMembers.coalesce(10).write()
        .mode(SaveMode.Overwrite)
        .save("steamProject/GroupsPlayers");


    // Store what Groups a Steam Id belongs to
    Dataset<Row> playersGroups = playerGroups.groupBy("steamid")
        .agg(collect_list("groupid").alias("groups"), count("groupid").alias("NumberOfGroups"));

    playersGroups.show();
    playerGroups.printSchema();


    playersGroups.coalesce(10).write()
        .mode(SaveMode.Overwrite)
        .save("steamProject/PlayersGroups");


  }

}

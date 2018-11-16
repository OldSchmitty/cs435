package cs435.dataproccessing;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class FindGroupNetWorth {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("FindGroupNetWorth:: needs a path <PlayerNetWorth> <GroupInfo>");
    }

    String playerNetWorthDir = args[0];
    String groupInfoDir = args[1];


    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - NetWorth")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");


    Dataset playerNetWorth = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerNetWorthDir);

    Dataset groupInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(groupInfoDir);


    System.out.println("There are " + playerNetWorth.count() + " player's with a calculated net worth in the file");
    playerNetWorth.show(10);
    playerNetWorth.printSchema();

    System.out.println("There are " + groupInfo.count() + " group items in the file");
    groupInfo.show(10);
    groupInfo.printSchema();

    Dataset playerGroups = groupInfo.select("steamid", "groupid").join(playerNetWorth, "steamid");

    playerGroups.show(10);
    playerGroups.summary();

    Dataset<Row> groupNetWorth = playerGroups.groupBy("groupid")
        .agg(count("steamid"), sum("NetWorth"), avg("NetWorth"))
        .withColumn("AverageMemberNetWorth", col("avg(NetWorth)"))
        .withColumn("NetWorthOfGroup",col("sum(NetWorth)"))
        .withColumn("NumberOfMembers",col("count(steamid)"))
        .drop("count(steamid)", "sum(NetWorth)", " avg(NetWorth)");

    groupNetWorth.orderBy(col("NumberOfMembers").desc()).show(30);
    groupNetWorth.select("AverageMemberNetWorth", "NetWorthOfGroup", "NumberOfMembers").summary().show();




    /*

    gamePrices.select("Price").summary().show();


    userGames = userGames.select(col("steamid"), col("appid"));

    userGames = userGames.join(gamePrices, "appid");

    Dataset<Row> userNetWorth = userGames.groupBy("steamid")
        .agg(count("appid"), sum("Price")).withColumn("NumberOfGames", col("count(appid)"))
        .withColumn("NetWorth",col("sum(Price)")).drop("count(appid)", "sum(Price)");

    userNetWorth.show(20);

    Dataset<Row> netWorthSum = userNetWorth.select("NumberOfGames", "NetWorth").
        summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
            "99%");

    netWorthSum.show(20);

    userNetWorth.coalesce(1).write()
        .mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
        .option("header", "true")
        .save("steamProject/PlayerNetWorth");

    netWorthSum.coalesce(1).write()
        .mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
        .option("header", "true")
        .save("steamProject/PlayerNetWorthSum");
        */
  }

}

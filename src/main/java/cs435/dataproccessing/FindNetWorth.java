package cs435.dataproccessing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindNetWorth {


  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("FindNetWorth:: needs a path <PlayerGames> <GameInfo>");
    }

    String playerGamesDir = args[0];
    String gameInfoDir = args[1];


    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - NetWorth")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");


    Dataset userGames = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerGamesDir);

    Dataset gameInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(gameInfoDir);


    System.out.println("There are " + userGames.count() + " player games in the file");
    userGames.show(10);
    userGames.printSchema();

    System.out.println("There are " + gameInfo.count() + " info on games in the file");
    gameInfo.show(10);
    gameInfo.printSchema();

    Dataset gamePrices = gameInfo.select(col("appid"), col("Price")).withColumn("Price", col("Price").cast(DoubleType));

    gamePrices.show(10);
    gamePrices.printSchema();

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

  }


}

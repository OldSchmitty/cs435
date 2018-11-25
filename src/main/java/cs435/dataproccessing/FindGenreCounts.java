package cs435.dataproccessing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FindGenreCounts {

  public static void main(String[] args) throws Exception {


    String apIdDir = args[0];
    String genreInfoDir = args[1];
    String playerInfoDir = args[2];


    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - GenreCounts")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    Dataset genreInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(genreInfoDir);

    Dataset gameInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(apIdDir);

    Dataset playerInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerInfoDir);

    genreInfo.show();
    gameInfo.show();
    playerInfo.show();

    Dataset gamePrices = gameInfo.select(col("appid"), col("Price")).withColumn("Price", col("Price").cast(DoubleType));

    Dataset userGames = playerInfo.select(col("steamid"), col("appid"), col("playtime_forever"));

    userGames = userGames.join(gamePrices, "appid").join(genreInfo, "appid");

    Dataset<Row> usersGenres = userGames.stat().crosstab("steamid", "Genre")
        .withColumn("steamid", col("steamid_Genre")).drop("steamid_Genre");

    usersGenres.show();


    Dataset<Row> userNetWorth = userGames.groupBy("steamid")
        .agg(count("appid").alias("NumberOfGamesOwned"),
            sum("Price").alias("NetWorth"),
            sum("playtime_forever").alias("TotalPlayTime"));

    userNetWorth.show();

    userNetWorth.select("TotalPlayTime").summary().show();

    userNetWorth.join(usersGenres, "steamid").show();



  }
}

package cs435.dataproccessing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindNetWorth {


  public static void main(String[] args) throws Exception {

    if (args.length < 4) {
      throw new IllegalArgumentException(
          "FindNetWorth:: needs a path <PlayerGames> <GameInfo> <PlayerSummary> <Genre>");
    }

    // Get file locations
    String playerGamesDir = args[0];
    String gameInfoDir = args[1];
    String userInfoDir = args[2];
    String genreInfoDir = args[3];

    // Start Spark job
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - NetWorth")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    // Read Files
    Dataset userGames = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerGamesDir);

    Dataset gameInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(gameInfoDir);


    Dataset userInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(userInfoDir);

    Dataset genreInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(genreInfoDir);



    // Count takes 5+ mins
    // Check to see if data was read properly
    System.out.println("There are " + userGames.count() + " player games in the file");
    userGames.show(10);
    userGames.printSchema();

    System.out.println("There are " + gameInfo.count() + " info on games in the file");
    gameInfo.show(10);
    gameInfo.printSchema();

    System.out.println("There are " + userInfo.count() + " users in the file and " + userInfo.filter(col("timecreated").equalTo("NULL")).count() + " players with no creation date");
    userInfo.show(10);
    userInfo.printSchema();


    // Change price from string to double
    Dataset gamePrices = gameInfo.select(col("appid"), col("Price"))
        .withColumn("Price", col("Price").cast(DoubleType));
    gamePrices.show(10);
    gamePrices.printSchema();

    // Get data from the users games
    userGames = userGames.select(col("steamid"), col("appid"), col("playtime_forever"));

    Dataset baseUserGameInfo = userGames.join(gamePrices, "appid").
        join(genreInfo, "appid");


    // Calculate the net worth and total play time by Steam Id
    Dataset<Row> userNetWorth = baseUserGameInfo.groupBy("steamid")
        .agg(count("appid").alias("NumberOfGamesOwned"),
            sum("Price").alias("NetWorth"),
            sum("playtime_forever").alias("TotalPlayTime"));

    // Get the age of the account
    userInfo = userInfo.withColumn("timecreated", to_date(col("timecreated")));

    // Make output file
    Dataset userInfoOutput = userInfo.select("steamid", "timecreated")
        .join(userNetWorth, "steamid");

    userInfoOutput.show();

    userInfoOutput.write()
        .mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
        .option("header", "true")
        .save("steamProject/PlayerAccountData");


    // Get general info on SteamId

    Dataset<Row> usersGameGenres = baseUserGameInfo.stat().crosstab("steamid", "Genre")
        .withColumn("steamid", col("steamid_Genre")).drop("steamid_Genre");

    // Fix Genre to have no Spaces in the name
    for(String name : usersGameGenres.columns())
    {
      usersGameGenres = usersGameGenres.withColumnRenamed(name, name.replaceAll(" ", ""));
    }

    usersGameGenres.show();


    Dataset<Row> userGamesAndGenres = baseUserGameInfo.groupBy("steamid")
        .agg(count("appid").alias("NumberOfGamesOwned"),
            collect_list("appid").alias("Games")).join(usersGameGenres, "steamid");

    userGamesAndGenres.show();

     userGamesAndGenres.coalesce(5).write()
        .mode(SaveMode.Overwrite)
        .save("steamProject/PlayerGamesAndGenres");

  }


}

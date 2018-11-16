package cs435.dataproccessing;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindCountInfo {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("FindNetWorth:: needs a path <PlayerGames> <GameInfo>");
    }

    String playerGamesDir = args[0];
    String gameInfoDir = args[1];
    String playerInfoDir = args[2];
    String playerNetWorthDir = args[3];

    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - NetWorth")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");


    Dataset gameInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(gameInfoDir);

    Dataset playerInfo = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerInfoDir);

    Dataset playerNetWorth = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(playerNetWorthDir);

    Dataset richAccounts = playerNetWorth.filter("NetWorth > 10000");

    richAccounts.show();

    playerInfo.join(richAccounts, "steamid").show();


  }

}

package cs435.regression;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class playTime_numOfGroupsVsNetWorth {


    public static void main(String[] args){
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Whales Vs Shrimp - Regression")
                    .master("local")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");
            SparkContext sc = spark.sparkContext();
            String path = args[0];
            Dataset gameInfo = spark.read().format("csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load(path);

            gameInfo.summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
                    "99%").show();

            gameInfo.na().drop().summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
                    "99%").show();

            //System.out.println("The number of nulls in NumberOfGroups: " + gameInfo.select(col("NumberOfGroups").isNotNull()).count());
            //System.out.println("The number of nulls in AverageMemberNetWorth: " + gameInfo.select(col("NumberOfMembers")).count());
            gameInfo.show();
            gameInfo.printSchema();

            gameInfo = gameInfo.na().drop();
            //load in file and change for the appropriate columns in real dataset
            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"NumberOfGroups","NumberOfGamesOwned","TotalPlayTime"})   //number of games owned by a user
                    .setOutputCol("regressionInputVector");          //set to vector for the regression input
            System.out.println("The correlation of NetWorth and NumberOfGroups is: " + gameInfo.stat().corr("NetWorth","NumberOfGroups"));
            System.out.println("The correlation of NetWorth and NumberOfGamesOwned is: " + gameInfo.stat().corr("NetWorth","NumberOfGamesOwned"));
            System.out.println("The correlation of NetWorth and TotalPlayTime is: " + gameInfo.stat().corr("NetWorth","TotalPlayTime"));

            Dataset<Row> vectorData = assembler.transform(gameInfo);
            vectorData.show();    //testing output to make sure we got here right

            LinearRegression lr = new LinearRegression().setLabelCol("NetWorth").setFeaturesCol("regressionInputVector");

            Dataset<Row>[] dataSplit = vectorData.randomSplit(new double[]{0.7, 0.3});

            LinearRegressionModel model = lr.fit(dataSplit[0]);

            System.out.println("Coefficients: " + model.coefficients() + " Intercept: " + model.intercept());
            Dataset<Row> predictions = model.transform(dataSplit[1]);
            predictions.select("NetWorth", "prediction").show();

        }
    }



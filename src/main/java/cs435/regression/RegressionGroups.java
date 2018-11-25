package cs435.regression;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RegressionGroups {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Whales Vs Shrimp - Regression")
                //.master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        SparkContext sc = spark.sparkContext();
        String path = args[0];
        Dataset gameInfo = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);

        gameInfo.filter("NumberOfMembers < 1000").summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
            "99%").show();

        gameInfo.summary("count", "mean", "stddev", "min", "max", "1%", "10%", "25%", "50%", "75%", "90%",
            "99%").show();


        System.out.println("The number of nulls in NumberOfMembers: " + gameInfo.select(col("NumberOfMembers").isNotNull()).count());
        System.out.println("The number of nulls in AverageMemberNetWorth: " + gameInfo.select(col("NumberOfMembers")).count());
        gameInfo.show();
        gameInfo.printSchema();

        gameInfo = gameInfo.filter("AverageMemberNetWorth is not null");
        //load in file and change for the appropriate columns in real dataset
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"NumberOfMembers"})   //number of members in a group
                .setOutputCol("numOfPlayersVector");          //set to vector for the regression input

        Dataset<Row> vectorData = assembler.transform(gameInfo);
        vectorData.show();    //testing output to make sure we got here right
        vectorData.orderBy("AverageMemberNetWorth").filter(vectorData.col("NumberOfMembers").isNotNull()).show();
        vectorData.orderBy("NumberOfMembers").show();
        LinearRegression lr = new LinearRegression().setLabelCol("AverageMemberNetWorth").setFeaturesCol("numOfPlayersVector");
        LinearRegressionModel model = lr.fit(vectorData);
        double[] predictedMembers = new double[]{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};

        System.out.println("Coefficients: " + model.coefficients() + " Intercept: " + model.intercept());


        for(double groupSize: predictedMembers){
            Vector predictions = Vectors.dense(new double[]{groupSize});    //add all predictions we want here.
            System.out.println("For a group of size " + groupSize + " the predicted average member net worth is " + model.predict(predictions));
        }

        gameInfo = gameInfo.filter("NetWorthOfGroup is not null");
        //load in file and change for the appropriate columns in real dataset
        assembler = new VectorAssembler()
                .setInputCols(new String[]{"NumberOfMembers"})   //number of members in a group
                .setOutputCol("numOfPlayersVector");          //set to vector for the regression input

        vectorData = assembler.transform(gameInfo);
        vectorData.show();    //testing output to make sure we got here right
        Dataset<Row>[] dataSplit = vectorData.randomSplit(new double[]{0.7, 0.3});
        lr = new LinearRegression().setLabelCol("NetWorthOfGroup").setFeaturesCol("numOfPlayersVector");
        model = lr.fit(dataSplit[0]);
        System.out.println("Coefficients: " + model.coefficients() + " Intercept: " + model.intercept());

        Dataset<Row> results = model.transform(dataSplit[1])
            .select("NumberOfMembers", "AverageMemberNetWorth", "prediction").withColumn("Dif%",
                col("AverageMemberNetWorth").minus(col("prediction"))
                    .divide(col("AverageMemberNetWorth")));

        results.select("Dif%").summary().show();

        for(double groupSize: predictedMembers){
            Vector predictions = Vectors.dense(new double[]{groupSize});    //add all predictions we want here.
            System.out.println("For a group of size " + groupSize + " the predicted group net worth is " + model.predict(predictions));
        }
    }
}

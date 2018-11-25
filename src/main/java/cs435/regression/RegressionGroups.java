package cs435.regression;

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
                .master("local")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        String path = args[0];
        Dataset gameInfo = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"NumberOfMembers"})   //number of members in a group
                .setOutputCol("numOfPlayersVector");          //set to vector for the regression input

        Dataset<Row> vectorData = assembler.transform(gameInfo);
        vectorData.show();    //testing output to make sure we got here right
        LinearRegression lr = new LinearRegression().setLabelCol("AverageMemberNetWorth").setFeaturesCol("numOfPlayersVector");
        LinearRegressionModel model = lr.fit(vectorData);
        Vector predictions = Vectors.dense(new double[]{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000});    //add all predictions we want here.
        System.out.println(model.predict(predictions));
    }
}

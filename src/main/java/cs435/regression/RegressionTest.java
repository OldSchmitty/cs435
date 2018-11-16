package cs435.regression;

import com.sun.rowset.internal.Row;
import org.apache.spark.SparkContext;

import org.apache.spark.ml.regression.LinearRegression;

import org.apache.spark.mllib.regression.LinearRegressionModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class RegressionTest {

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
    gameInfo.show();
    gameInfo.printSchema();

    LinearRegression lr = new LinearRegression();
    LinearRegressionModel model = lr.fit(gameInfo);



    /*JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
    training.cache();
    JavaRDD<LabeledPoint> test = data.subtract(training);

    // Run training algorithm to build the model.
    int numIterations = 100;
    SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

    // Clear the default threshold.
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p ->
        new Tuple2<>(model.predict(p.features()), p.label()));

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics =
        new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    double auROC = metrics.areaUnderROC();

    System.out.println("Area under ROC = " + auROC);

// Save and load model
    model.save(sc, "target/tmp/javaSVMWithSGDModel");
    SVMModel sameModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel");
    */
  }



}

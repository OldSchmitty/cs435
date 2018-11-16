package cs435.regression;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class RegressionTest {

  public static void main(String[] args){
    SparkSession spark = SparkSession
        .builder()
        .appName("Whales Vs Shrimp - Regression")
        .getOrCreate();

    SparkContext sc = spark.sparkContext();

    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

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
  }



}

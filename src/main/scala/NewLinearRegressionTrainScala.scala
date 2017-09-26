import com.irfan.AlertPayloadTransformer
import ml.combust.bundle.BundleFile
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.feature.{RFormula, VectorAssembler, VectorSlicer}
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ml.combust.mleap.spark.SparkSupport._
import resource._
object NewLinearRegressionTrainScala {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.appName("Java Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate
    // Load training data
    val dataset = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .load("random_sel.csv")

    val formula = new RFormula().setFormula("Class ~ .").setFeaturesCol("features").setLabelCol("label")

    val training = formula.fit(dataset).transform(dataset)


    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    val vectorSlicer = new VectorAssembler().setInputCols(Array[String]("ORIGIN_COUNTRY_US_IND", "ORIGIN_COUNTRY_NONUS_NONSANCTIONS_IND", "ORIGIN_COUNTRY_SANCTIONS_IND", "ORIGIN_CUSTOMER_INDIVIDUAL_IND", "ORIGIN_CUSTOMER_BUSINESS_IND", "ORIGIN_RISK_SCORE_1_IND", "ORIGIN_RISK_SCORE_2_IND", "ORIGIN_RISK_SCORE_3_IND", "ORIGIN_RISK_SCORE_4_IND", "ORIGIN_RISK_SCORE_5_IND", "BENEFICIARY_COUNTRY_US_IND", "BENEFICIARY_COUNTRY_NONUS_NONSANCTIONS_IND", "BENEFICIARY_COUNTRY_SANCTIONS_IND", "BENEFICIARY_CUSTOMER_INDIVIDUAL_IND", "BENEFICIARY_CUSTOMER_BUSINESS_IND", "BENEFICIARY_RISK_SCORE_1_IND", "BENEFICIARY_RISK_SCORE_2_IND", "BENEFICIARY_RISK_SCORE_3_IND", "BENEFICIARY_RISK_SCORE_4_IND", "BENEFICIARY_RISK_SCORE_5_IND", "ORIGIN_BASIC_CHECKINGS_IND", "ORIGIN_CERTIFICATE_OF_DEPOSIT_IND", "ORIGIN_INTEREST_BEARING_CHECKING", "ORIGIN_MONEY_MARKET_DEPOSIT", "ORIGIN_SAVINGS_ACCOUNT", "BENEFICIARY_BASIC_CHECKINGS_IND", "BENEFICIARY_CERTIFICATE_OF_DEPOSIT_IND", "BENEFICIARY_INTEREST_BEARING_CHECKING", "BENEFICIARY_MONEY_MARKET_DEPOSIT", "BENEFICIARY_SAVINGS_ACCOUNT", "ATM_IND", "CHARGE_IND", "CHECK_IND", "DEPOSIT_IND", "POS_IND", "TRANSFER_IND", "WITHDRAWAL_IND", "ACH_IND", "DEPOSIT_SELF_SERVICE_IND", "MERCHANT_LOCATION_IND", "ONLINE_IND", "TELLER_IND", "SELF_SERVICE_IND", "WIRE_IND", "WITHDRAWAL_SELF_SERVICE_IND", "TRANSACTION_AMT_0_5K_IND", "TRANSACTION_AMT_5K_10K_IND", "TRANSACTION_AMT_10K_50K_IND", "TRANSACTION_AMT_50K_100K_IND", "TRANSACTION_AMT_100K_500K_IND", "TRANSACTION_AMT_500K_1M_IND", "TRANSACTION_AMT_GREATER_1M_IND")).setOutputCol("features")
    val apT = new AlertPayloadTransformer();
    val dataset2 = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .load("prodtests.csv")

    val preTrain = vectorSlicer2.transform(dataset2)

    val training2 = preTrain//formula.fit(preTrain).transform(preTrain)

    val pRes = lrModel.transform(training2)

    pRes.show();

    val apTRes = apT.transform(pRes)

    apTRes.show();

    System.out.println(apTRes.select("desc").first())



//    val export_pipeline = SparkUtil.createPipelineModel(uid = "pipeline",Array(vectorSlicer, lrModel,apT))
//
//    val sbc = SparkBundleContext()
//    for(bf <- managed(BundleFile("jar:file:/tmp/aml.model.rf.zip"))) {
//      export_pipeline.writeBundle.save(bf)(sbc).get
//    }
  }
}

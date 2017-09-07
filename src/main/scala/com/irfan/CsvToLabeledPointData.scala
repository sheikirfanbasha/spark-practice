import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
object CsvToLabeledPointData {
	def main(args : Array[String]){
		val conf = new SparkConf().setAppName("LinearRegressionTrain").setMaster("local")
          .setSparkHome("/usr/local/spark").set("spark.executor.memory", "4g").set("driver-memory","4g");
    	val sc = new SparkContext(conf)
		//To read the file
		val csv = sc.textFile("sample_aml_data.csv");

		//To find the headers
		val header = csv.first;

		//To remove the header
		val data = csv.filter(_(0) != header(0));

		//To create a RDD of (label, features) pairs
		val parsedData = data.map { line =>
		    val parts = line.split(',')

		    LabeledPoint(parts(parts.length - 1).toDouble, Vectors.dense(parts.slice(1, parts.length - 1).mkString(",").split(',').map(_.toDouble)))
		    }.cache()

		parsedData.map { lp =>
		  System.out.println(lp.features)
		}

		parsedData.saveAsTextFile("samp_aml_libsvm.txt")

	}
}
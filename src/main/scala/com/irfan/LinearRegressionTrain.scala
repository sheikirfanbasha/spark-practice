import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.util.Base64
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object LinearRegressionTrain {

  // /** Read the object from Base64 string. */
  //  private static Object fromString( String s ) throws IOException ,
  //                                                      ClassNotFoundException {
  //       byte [] data = Base64.getDecoder().decode( s );
  //       ObjectInputStream ois = new ObjectInputStream( 
  //                                       new ByteArrayInputStream(  data ) );
  //       Object o  = ois.readObject();
  //       ois.close();
  //       return o;
  //  }

  //   /** Write the object to a Base64 string. */
  //   private static String toString( Serializable o ) throws IOException {
  //       ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //       ObjectOutputStream oos = new ObjectOutputStream( baos );
  //       oos.writeObject( o );
  //       oos.close();
  //       return Base64.getEncoder().encodeToString(baos.toByteArray()); 
  //   }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegressionTrain").setMaster("local")
          .setSparkHome("/usr/local/spark").set("spark.executor.memory", "4g").set("driver-memory","4g");
    val sc = new SparkContext(conf)
	// Load training data in LIBSVM format.
	//val data = MLUtils.loadLibSVMFile(sc, "samp_am_libsvm.txt")

	//Load csv and conver to LabeledPointData
	val csv = sc.textFile("/Users/irfan/Personal/Project/spark-practice/sample_aml_data.csv");

	//To find the headers
	val header = csv.first;

	//To remove the header
	val _data = csv.filter(_(0) != header(0));

	//To create a RDD of (label, features) pairs
	val parsedData = _data.map { line =>
	    val parts = line.split(',')
	    System.out.println(parts.length)
	    val f = Vectors.dense(parts.slice(0, parts.length - 1).mkString(",").split(',').map(_.toDouble))
	    //System.out.println(f.toString())
	    LabeledPoint(parts(parts.length - 1).toDouble, f)
	}
// Split data into training (60%) and test (40%).
val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0)
val test = splits(1)

// Run training algorithm to build the model
val numIterations = 100
val model = SVMWithSGD.train(training, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set.
val scoreAndLabels = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

for ((k,v) <- scoreAndLabels) printf("key: %s, value: %s\n", k, v)
// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)

//save model to file
val fout = new FileOutputStream("/Users/irfan/Documents/model.ser")
val baos = new ByteArrayOutputStream()
val oos = new ObjectOutputStream( baos )
oos.writeObject( model )
oos.close();
val res = Base64.getEncoder().encodeToString(baos.toByteArray())
System.out.println(res)
//val fos = new ObjectOutputStream(fout)
//fos.writeObject(res)
//fos.close();

// Save and load model
//model.save(sc, "target/tmp/scalaSVMWithSGDModel")
//val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")

 sc.stop()
  }
}
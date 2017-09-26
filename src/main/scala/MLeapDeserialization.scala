import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.serialization.FrameReader
import resource._
import ml.combust.mleap.tensor.SparseTensor;
package com.irfan {

  object MLeapDeserialization {
    def process(path: String): Double = {
      var result = 0.0;
      val model = (for (bf <- managed(BundleFile(path))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get

      val s = scala.io.Source.fromURL("file:///Users/irfan/Downloads/aml_mleapframe.json").mkString

      val bytes = s.getBytes("UTF-8")

      for (frame <- FrameReader("ml.combust.mleap.json").fromBytes(bytes);
           frameLr <- model.transform(frame);
           frameLrSelect <- frameLr.select("prediction")) {
        System.out.println("working")
        System.out.println("done")
        frameLr.printSchema()
        result = frameLrSelect.dataset(0).getDouble(0);
        println("Predicted value: " + result)
      }
      return result;
    }

    def main(args: Array[String]): Unit = {
      process("jar:file:/tmp/aml.model.rf.zip");
    }
  }

}

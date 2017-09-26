package com.irfan

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

class AlertPayloadTransformer extends  Transformer{
  override def transform(dataset: Dataset[_]) = {
    transformSchema(dataset.schema)
    val x = dataset.select("BENEFICIARY_ACCOUNT_ID")
    val acc_num = x.first()
    var res = dataset.select(dataset.col("prediction").as("score"), dataset.col("BENEFICIARY_ACCOUNT_ID").as("susp_acc_num"), dataset.col("CHANNEL_ID").as("channelId"))
    res = res.withColumn("sub", lit("Predicts"))
    res = res.withColumn("alertType", lit("ModelGenerated"))
    res = res.withColumn("desc", lit(s"This is the alert generated for the account number $acc_num"))
    res = res.withColumn("priority", lit("high"))
    res = res.withColumn("status", lit("open"))
    res = res.withColumn("correlation", typedLit(Seq("rec1")))
    res
  }

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val pIdx = schema.fieldIndex("prediction")
    val pField = schema.fields(pIdx)
    if (pField.dataType != DoubleType) {
      throw new Exception(s"Input prediction field type ${pField.dataType} did not match input type DoubleType")
    }
    // Add the return field
    schema.add(StructField("sub", StringType, false))
    schema.add(StructField("modelId", StringType, false))
    schema.add(StructField("predicate", StringType, false))
    schema.add(StructField("susp_acc_num", StringType, false))
    schema.add(StructField("alertType", StringType, false))
    schema.add(StructField("desc", StringType, false))
    schema.add(StructField("priority", StringType, false))
    schema.add(StructField("status", StringType, false))
    schema.add(StructField("score", DoubleType, false))
    schema.add(StructField("correlation", StructType(List(StructField("account_id",StringType,false)))))
    schema.add(StructField("channelId", DoubleType, false))

  }

  override val uid = Identifiable.randomUID("alertPayloadTransformer")
}

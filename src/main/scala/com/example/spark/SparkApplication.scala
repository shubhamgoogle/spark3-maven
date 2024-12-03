package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object SparkApplication extends App{
  val sparkSession = SparkSession.builder().appName("sample-spark").master("local[*]").getOrCreate()
  import sparkSession.implicits._
  sparkSession.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

  var logger:org.slf4j.Logger=LoggerFactory.getLogger(this.getClass)

  // Read from BQ table
  logger.info("Reading BQ table: customer_data using Spark BQ Connector")

  val df1 = sparkSession.read.format("bigquery")
    .load("dataproc_demo.customer_data")

  logger.info("Sample Data:")
  df1.show()

  // Write to BQ table
  //Direct
  logger.info("Writing to BQ table: customer_data_direct using Spark BQ Connector DIRECT method")
  df1.write.format("bigquery").option("writeMethod", "direct").mode("overwrite").save("dataproc_demo.customer_data_direct")
  logger.info("Direct Write Completed Successfully")


  // Write to BQ table
  //Indirect
  logger.info("Writing to BQ table: customer_data_indirect using Spark BQ Connector INDIRECT method")
  df1.write.format("bigquery").option("temporaryGcsBucket","datapoc-demo").mode("overwrite").save("dataproc_demo.customer_data_indirect")
  logger.info("Indirect Write Completed Successfully")



}

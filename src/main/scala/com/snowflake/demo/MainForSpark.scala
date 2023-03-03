package com.snowflake.demo

import com.snowflake.demo.util.delta.{DeltaTableMetaData, DeltaTableReaderAndWriter}
import com.snowflake.demo.util.{ApplicationHelper, SnowFlakeConnectionInfo, SparkSessionApp}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object MainForSpark extends SparkSessionApp{
  private lazy val snowFlakeConnectionInfo = ApplicationHelper.loadSnowflakeProperties
  private lazy val tableName = "EV_Data"
  private lazy val s3BucketName = "yuetest123"
  private lazy val objectPath = "data/electric_vehicle_population_data.json"

  //set up necessary configuration parameters in order to access AWS S3
  def setUpS3AccessSettings = {
    val  o = ApplicationHelper.loadAWSProperties
    val accessKey = o.awsAccessKey
    val secretKey = o.awsSecretKey
    sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkContext.hadoopConfiguration.set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")

  }

  //read json file from AWS S3 storage
  def loadJsonFileIntoDataFrame(s3BucketName: String, fileName: String): DataFrame = {
    val path = s"s3a://$s3BucketName/$fileName"
    spark.read.option("multiline", "true").json(path)
  }

  override def runApp(): Unit= {
    //set up AWS S3 access key and secret key
    setUpS3AccessSettings


    // Read the JSON file which is in the cloud storage S3 and  parse and extract all the elements of the Json object.
    val jsonDF = loadJsonFileIntoDataFrame(s3BucketName = s3BucketName, fileName = objectPath)
    jsonDF.printSchema()

    //cache dataframe for iterative use
    jsonDF.cache()

    //flatten data into individual columns
    val dataDF = ApplicationHelper.getMasterData(jsonDF = jsonDF)
    dataDF.printSchema()
    dataDF.show(false)

    //flatten approvals data into individual columns
    val approvalDF = ApplicationHelper.getDataFrameForApprovals(jsonDF = jsonDF)
    approvalDF.printSchema()
    approvalDF.show(false)

    //create snowflake table based on columns information from meta using JDBC
    ApplicationHelper.createTableOnSnowFlake(
      snowFlakeConnectionInfo = snowFlakeConnectionInfo,
      tableName = tableName,
      columns = dataDF.schema.fields.toList)

    //save data dataframe into table on snowflake
    ApplicationHelper.saveMastDataIntoSnowFlake(dataDF = dataDF, snowFlakeConnectionInfo = snowFlakeConnectionInfo, tableName = tableName)

    val targetFinalDeltaTableMetaData = DeltaTableMetaData(deltaBaseLocationPath = s"s3a://$s3BucketName", deltaSchemaName = "ev", deltaTableName = Option("ev_data"))
    //save data dataframe into delta table on S3
    DeltaTableReaderAndWriter(spark).writeIntoFinalTable(df = dataDF, primaryKeys = Array[String]("id"), targetFinalDeltaTableMetaData = targetFinalDeltaTableMetaData)

    DeltaTable.forPath(targetFinalDeltaTableMetaData.deltaTableFullStoragePath).toDF.show(false)
    //release memory cache
    jsonDF.unpersist()
  }
}

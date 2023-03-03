package com.snowflake.demo.util

import com.snowflake.demo.util.json.FlattenJson
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, element_at, explode, struct, transform}
import org.apache.spark.sql.types.{FloatType, StringType, StructField}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.sql.DriverManager
import java.util.Properties
import scala.collection.JavaConverters._

object ApplicationHelper {

  def loadAWSProperties = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/config/aws.properties"))
    AwsCredentialInfo(awsAccessKey =  properties.getProperty("AWS_ACCESS_KEY"),
      awsSecretKey =  properties.getProperty("AWS_SECRET_KEY"))
  }

  def loadSnowflakeProperties = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/config/snowflake.properties"))
    SnowFlakeConnectionInfo(url = properties.getProperty("URL"),
      user = properties.getProperty("USER"),
      password = properties.getProperty("PASSWORD"),
      role = properties.getProperty("ROLE"),
      warehouse = properties.getProperty("WAREHOUSE"),
      database = properties.getProperty("DB"),
      schema = properties.getProperty("SCHEMA"),
      account = properties.getProperty("ACCOUNT"))
  }


  def anlayzeDataAndDescribeAllFields(jsonDF: DataFrame): Unit = {
    //Analyze the data and describe all the fields you will be extracting to derive meaningful
    //insights from the dataset.
    jsonDF.printSchema()
  }

  def getStructFieldList(jsonDF: DataFrame): List[StructField] = {
    import jsonDF.sparkSession.implicits._

    //get all fields(columns) dynamically
    val df = jsonDF
      .select(col("meta.view.columns").as("columns"))
      .withColumn("columns_exploded", explode(col("columns")))
      .select("columns_exploded.*")
      .select("dataTypeName", "fieldName", "name")


    val columns = df.collectAsList().asScala.map { row =>
      val dataTypeName = row.getAs[String]("dataTypeName").trim
      val fieldName = row.getAs[String]("fieldName").trim.replaceAll("[:|@]", "")
      StructField(name = fieldName,
        dataType = if (dataTypeName.toLowerCase.equals("number")) FloatType else StringType, nullable = true)
    }.toList
    columns
  }

  def getMasterData(jsonDF: DataFrame) = {
    val columns = getStructFieldList(jsonDF = jsonDF)

    val df = jsonDF.select("data")

    def t(c: Column, structFields: List[StructField]): Column = {
      val cols = (0 until structFields.size).map { index =>
        element_at(c, index + 1).cast(structFields(index).dataType).as(structFields(index).name)
      }
      struct(cols: _*)
    }

    val dataDF = df.withColumn("data_exploded", transform(col("data"), t(_, columns)))
      .withColumn("data_exploded", explode(col("data_exploded")))
      .select("data_exploded.*")

    dataDF.printSchema()

    dataDF

  }


  def createTableOnSnowFlake(snowFlakeConnectionInfo: SnowFlakeConnectionInfo, tableName: String, columns: List[org.apache.spark.sql.types.StructField]) = {
    val properties = new java.util.Properties()
    properties.put("user", snowFlakeConnectionInfo.user)
    properties.put("password", snowFlakeConnectionInfo.password)
    properties.put("account",  snowFlakeConnectionInfo.account)
    properties.put("warehouse", snowFlakeConnectionInfo.warehouse)
    properties.put("db",  snowFlakeConnectionInfo.database)
    properties.put("schema", snowFlakeConnectionInfo.schema)
    properties.put("role", snowFlakeConnectionInfo.role)

    val sql = new StringBuffer()
    sql.append(s"create or replace table $tableName(").append("\n")
    val s = columns.map(field =>{
      val dataType = if (field.dataType == StringType) "VARCHAR" else "number"
      s"    ${field.name} $dataType"
    }).mkString(",\n")

    sql.append(s)
    sql.append(")").append("\n")

    //JDBC connection string
    val jdbcUrl = s"jdbc:snowflake://${snowFlakeConnectionInfo.account}.snowflakecomputing.com"
    val connection = DriverManager.getConnection(jdbcUrl, properties)
    val statement = connection.createStatement
    statement.executeUpdate(sql.toString)
    statement.close
    connection.close()
  }


  def saveMastDataIntoSnowFlake(dataDF: DataFrame, snowFlakeConnectionInfo: SnowFlakeConnectionInfo, tableName: String) = {

    val configs = Map(
      "sfURL" -> snowFlakeConnectionInfo.url,
      "sfUser" -> snowFlakeConnectionInfo.user,
      "sfPassword" -> snowFlakeConnectionInfo.password,
      "sfRole" -> snowFlakeConnectionInfo.role,
      "sfDatabase" -> snowFlakeConnectionInfo.database,
      "sfSchema" -> snowFlakeConnectionInfo.schema,
      "sfAccount" -> snowFlakeConnectionInfo.account
    )


    dataDF.write
      .format("snowflake")
      .options(configs)
      .option("dbtable", tableName)
      .mode(SaveMode.Overwrite)
      .save()

  }

  def getDataFrameForApprovals(jsonDF: DataFrame) = {
    val df = jsonDF.withColumn("approval_array", col("meta.view.approvals"))
      .withColumn("approval_explode", explode(col("approval_array")))
      .select("approval_explode.*")
    //flatten the json data
    FlattenJson.flattenDataframe(inDF = df, ignoredFields = Seq.empty)
  }





}

package com.snowflake.demo

import com.snowflake.demo.util.ApplicationHelper
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructField}


object MainForSnowPark {
  private lazy val snowFlakeConnectionInfo = ApplicationHelper.loadSnowflakeProperties
  private lazy  val stage = "@s3_json_stage"
  private lazy val tableName = "ev_data_by_snowpark"

  def copyFileFromLocalIntoStage(session: Session, localFile: String, stageName: String) = {
        val map = Map[String, String]("AUTO_COMPRESS" -> "FALSE", "SOURCE_COMPRESSION" -> "NONE", "OVERWRITE" -> "TRUE")
        val rs = session.file.put(localFile, stageName, map)
  }

  def main(args: Array[String]): Unit = {
    //configure snowflake
    val configs = Map(
      "URL" -> snowFlakeConnectionInfo.url,
      "USER" -> snowFlakeConnectionInfo.user,
      "PASSWORD" -> snowFlakeConnectionInfo.password,
      "ROLE" -> snowFlakeConnectionInfo.role,
      "WAREHOUSE" -> snowFlakeConnectionInfo.warehouse,
     "DB" -> snowFlakeConnectionInfo.database,
     "SCHEMA" -> snowFlakeConnectionInfo.schema
    )
    val session = Session.builder.configs(configs).create


    //read all json files under the stage into dataframe and convert into object(variant) data type
    //val schema = com.snowflake.snowpark.types.StructType(Array(com.snowflake.snowpark.types.StructField("$1", com.snowflake.snowpark.types.VariantType)))
    val jsonDF = session
      .read
      //.option("multiline", "true")
      //.schema(schema)
      .json(stage)
      .select(to_object(col("$1")).as("value"))
    jsonDF.show()

    //get an array under json path $.meta.view.columns
    val colDF = jsonDF.select(col("value")("meta")("view")("columns").as("columns")).distinct()
    colDF.show()

    //flatten columns array into more rows
    val flattenColDF = colDF.flatten(col("columns")).select("value")

    //get dataTypeName and fieldName values from each column record
    val fieldNameTypeDF = flattenColDF.select(col("value")("dataTypeName").as("dataTypeName"),
      col("value")("fieldName").as("fieldName"))
    fieldNameTypeDF.show(100)


    //read data part from json data
    val dataDF = jsonDF.select(col("value")("data").as("data")).flatten(col("data")).select("value")
    dataDF.show

    //convert to tuple list
    val columnList = fieldNameTypeDF.collect().map(row=>{
      val dataTypeName = row.getString(0).trim.replace("\"", "")
      val fieldName = row.getString(1).trim.replaceAll("[:|@]", "").replace("\"", "")
      (fieldName, dataTypeName)
    }).toList
    val dfCols = columnList.indices.map {case index=>
      val item = columnList(index)
      col("value")(index).as(item._1)
    }

    //read each item from json object, and flatten a json array into multiple columns
    val flattenDataDF = dataDF.select(dfCols(0), dfCols.drop(1):_*)
    flattenDataDF.show()

    //create snowflake table based on columns information from meta using JDBC
    ApplicationHelper.createTableOnSnowFlake(
      snowFlakeConnectionInfo = snowFlakeConnectionInfo,
      tableName = tableName,
      columns = flattenDataDF.schema.fields.toList.map(x=>
        {
          val dataType = if (x.dataType == com.snowflake.snowpark.types.VariantType) StringType else FloatType
          StructField(name = x.name, dataType = dataType, nullable = true)
        }))


    val o = flattenDataDF.schema.fields.toList.map(x=>
    {
      val dataType = if (x.dataType == com.snowflake.snowpark.types.VariantType)
        com.snowflake.snowpark.types.StringType
      else com.snowflake.snowpark.types.FloatType
      col(x.name).cast(dataType).as(x.name)
    })

    //save data dataframe into table on snowflake
    val mDF = flattenDataDF.select(o(0), o.drop(1):_*)
    mDF.write.mode("overwrite").saveAsTable(tableName = tableName)

  }
}

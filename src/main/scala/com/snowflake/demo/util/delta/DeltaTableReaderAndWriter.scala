package com.snowflake.demo.util.delta

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.slf4j.LoggerFactory

case class DeltaTableReaderAndWriter(spark: SparkSession) extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def writeIntoFinalTable(df: DataFrame, targetFinalDeltaTableMetaData: DeltaTableMetaData, primaryKeys: Seq[String]) = {
    val sep = ","
    val finalDeltaTableFullStoragePath = targetFinalDeltaTableMetaData.deltaTableFullStoragePath
    val finalDeltaTableFullName = targetFinalDeltaTableMetaData.deltaTableFullQualifiedName
    logger.info(s"table name=${finalDeltaTableFullName}, primary keys=${primaryKeys.mkString(sep)}")
    import spark.implicits._

    val conditions = primaryKeys.map { case x => $"source.$x = target.$x" }.mkString(" and ")

    val window = Window.partitionBy(primaryKeys.map(col(_)): _*).orderBy(primaryKeys.map(col (_).desc):_*)
    val dataframe = df.withColumn("_row_number", row_number().over(window))
      .filter($"_row_number".equalTo(1))
      .drop("_row_number")


    if (DeltaTable.isDeltaTable(spark, finalDeltaTableFullStoragePath)) {
      logger.info(s"start merging delta table ${finalDeltaTableFullName}")
      val delta_table = DeltaTable.forPath(spark, finalDeltaTableFullStoragePath)
      delta_table.alias("target")
        .merge(dataframe.alias("source"), conditions)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
      logger.info(s"finish merging delta table ${finalDeltaTableFullName}")
    }
    else {
      logger.info(s"create delta table ${finalDeltaTableFullName} for the first time")
      val writer = dataframe
        .write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .option("path", finalDeltaTableFullStoragePath)
      writer.save()
      logger.info(s"finish writing into delta table ${finalDeltaTableFullName}")
    }
  }
}

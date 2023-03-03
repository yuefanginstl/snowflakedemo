package com.snowflake.demo.util.json

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object FlattenJson {
//  def renameDataFrameColumnName(inDF: DataFrame): DataFrame = {
//    val df = inDF.schema.fields.foldLeft(inDF)((df, fid)=>{
//      val newFid = fid.name
//      df.withColumnRenamed(fid.name, newFid)
//    })
//    df
//  }

  def replaceSpecialChars(fidName: String): String = {
    fidName.replace(".", "_").replace("@", "").replace("`", "")
  }

  def flattenDataframe(inDF: DataFrame, ignoredFields: Seq[String]): DataFrame = {
    val df = inDF //renameDataFrameColumnName(inDF)

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    for(i <- fields.indices){
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      if (!ignoredFields.contains(fieldName)){
        fieldType match {
          case _: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
            val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
            return flattenDataframe(explodedDf, ignoredFields)
          case structType: StructType =>
            val childFieldNames = structType.fieldNames.map(childName => fieldName + "." + childName)
            val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
            val renamedCols = newFieldNames.map(x => col(x).as(replaceSpecialChars(x)))
            val explodeDf = df.select(renamedCols: _*)
            return flattenDataframe(explodeDf, ignoredFields)
          case _ =>
        }
      }
    }
    df
  }
}

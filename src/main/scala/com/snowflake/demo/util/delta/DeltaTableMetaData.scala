package com.snowflake.demo.util.delta

case class DeltaTableMetaData(deltaBaseLocationPath: String, deltaSchemaName: String,
                             deltaTableName: Option[String] = None) extends Serializable {
  private val basePath = if (deltaBaseLocationPath.endsWith("/")) deltaBaseLocationPath.dropRight(1) else deltaBaseLocationPath

  def deltaTableFullStoragePath: String = {
    val s = s"$basePath/$deltaSchemaName"
    deltaTableName.map(x => s"$s/$x").getOrElse(s)
  }

  def deltaTableFullStoragePath(newTableName: String): String = {
    s"$basePath/$deltaSchemaName/$newTableName"
  }

  def deltaTableFullQualifiedName: String = {
    deltaTableName.map(x => s"$deltaSchemaName.$x").getOrElse("")//.getOrElse(throw new NoDeltaTableNameFoundException("no table name is provided"))
  }

  def deltaTableFullQualifiedName(newTableName: String): String = {
    s"$deltaSchemaName.$newTableName"
  }

}
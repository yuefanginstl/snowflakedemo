package com.snowflake.demo.util

case class SnowFlakeConnectionInfo(url: String, user: String, password: String, role: String, warehouse: String,
                                   database: String, schema: String, account: String) extends  Serializable


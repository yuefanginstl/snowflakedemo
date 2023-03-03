package com.snowflake.demo

object UrlContentDownloader {
  def convertUrlContentAsString(url: String): String ={
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.toString().stripLineEnd
    jsonResponseOneLine
  }
}

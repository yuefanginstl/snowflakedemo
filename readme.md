Snowflake homework

In this project, two spark applications are created.  
1. MainForSpark  
   src/main/scala/com/snowflake/demo/MainForSpark.scala  
  Use json files in S3 bucket as a data source, and run spark.  
  Save data into snowflake tables and delta tables on local machine
2. MainForSnowSpark  
   src/main/scala/com/snowflake/demo/MainForSnowPark.scala  
   Use internal stage as data source and run spark on it.  
   Save data into snowflake tables.
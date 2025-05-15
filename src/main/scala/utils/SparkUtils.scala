package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def executeSparkSql(spark: SparkSession, sql: String): Unit = {
    println(
      s"""
         |Executing:
         |--------------------------------
         |$sql
         |--------------------------------
         |""".stripMargin)

    spark.time {
      spark.sql(sql).show()
    }
  }
}

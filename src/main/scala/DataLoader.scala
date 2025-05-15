import DataLoader.{OPERATION_INSERT, OPERATION_UPSERT, SPARK_DATASOURCE_API, SPARK_SQL_API, withPersisted}

import datagen.ChangeDataGenerator

import io.delta.tables.DeltaTable

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import utils.SparkUtils.executeSparkSql
import utils.StringUtils.lineSepBold

import java.io.Serializable
import scala.collection.mutable.ListBuffer

class DataLoader(val spark: SparkSession, val numRounds: Int = 10) extends Serializable {

  private def tryCreateTable(schema: StructType,
                             outputPath: String,
                             format: String,
                             opts: Map[String, String],
                             nonPartitioned: Boolean,
                             scenarioId: String): Unit = {

    val tableName = format match {
      case "hudi" => genHudiTableName(scenarioId)
      case "iceberg" => genIcebergTableName(scenarioId)
    }
    val escapedTableName = escapeTableName(tableName)
    val targetPath = s"$outputPath/${tableName.replace('.', '/')}"

    dropTableIfExists(format, escapedTableName, targetPath)

    val serializedOpts = opts.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
    val createTableSql = format match {
      case "hudi" =>
        s"""
           |CREATE TABLE $escapedTableName (
           |  ${schema.toDDL}
           |)
           |USING HUDI
           |TBLPROPERTIES (
           |  tableType = 'cow',
           |  primaryKey = '${opts(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key)}',
           |  preCombineField = '${opts(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key)}',
           |  ${serializedOpts}
           |)
           |LOCATION '$targetPath'
           |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
           |""".stripMargin

      case "iceberg" =>
        s"""
           |CREATE TABLE $escapedTableName (
           |  ${schema.toDDL}
           |)
           |USING ICEBERG
           |TBLPROPERTIES (
           |  ${serializedOpts}
           |)
           |LOCATION '$targetPath'
           |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
           |""".stripMargin

      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported currently")
    }

    executeSparkSql(spark, createTableSql)
  }

  private def dropTableIfExists(format:String, escapedTableName: String, targetPathStr: String): Unit = {
    format match {
      case "iceberg" =>
        // Since Iceberg persists its catalog information w/in the manifest it's sufficient to just
        // drop the table from SQL
        executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")

      case "hudi" =>
        executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")

        val targetPath = new Path(targetPathStr)
        val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.delete(targetPath, true)
    }
  }


  def doWrites(inputPath: String,
               outputPath: String,
               parallelism: Int = 100,
               format: String = "parquet",
               operation: String = OPERATION_UPSERT,
               apiType: String = SPARK_DATASOURCE_API,
               opts: Map[String, String] = Map(),
               cacheInput: Boolean = false,
               overwrite: Boolean = true,
               nonPartitioned: Boolean = false,
               scenarioId: String,
               startRound: Int = 0): Unit = {
    println(
      s"""
         |$lineSepBold
         |Executing $scenarioId ($numRounds rounds)
         |$lineSepBold
         |""".stripMargin)

    val allRoundTimes = new ListBuffer[Long]()
    (startRound until numRounds).foreach(roundNo => {
      println(
        s"""
           |$lineSepBold
           |Writing round ${roundNo + 1} / $numRounds
           |$lineSepBold
           |""".stripMargin)

      val saveMode = if (roundNo == 0 && overwrite) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }

      val targetOperation = if (roundNo == 0) {
        OPERATION_INSERT
      } else {
        operation
      }

      val inputDF =
        spark.read.format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
          .load(s"$inputPath/$roundNo")

      if (cacheInput) {
        inputDF.cache()
        println(s"Cached ${inputDF.count()} records from $inputPath")
      }

      // Some formats (like Iceberg) do require to create table in the Catalog before
      // you are able to ingest data into it
      if (roundNo == 0 && (apiType == SPARK_SQL_API || format == "iceberg")) {
        tryCreateTable(inputDF.schema, outputPath, format, opts, nonPartitioned, scenarioId)
      }

      allRoundTimes += doWriteRound(inputDF, outputPath, parallelism, format, apiType, saveMode,
        targetOperation, opts, cacheInput, nonPartitioned, scenarioId)

      inputDF.unpersist()
    })

    println(
      s"""
         |$lineSepBold
         |Total time taken by all rounds (${format}): ${allRoundTimes.sum}
         |Per round: ${allRoundTimes.toList}
         |$lineSepBold
         |""".stripMargin)
  }

  def doWriteRound(inputDF: DataFrame,
                   outputPath: String,
                   parallelism: Int = 2,
                   format: String = "parquet",
                   apiType: String = SPARK_DATASOURCE_API,
                   saveMode: SaveMode = SaveMode.Append,
                   operation: String = OPERATION_UPSERT,
                   opts: Map[String, String] = Map(),
                   cacheInput: Boolean = false,
                   nonPartitioned: Boolean = false,
                   scenarioId: String): Long = {
    val startMs = System.currentTimeMillis()

    format match {
      case "hudi" =>
        val tableName = genHudiTableName(scenarioId)
        writeToHudi(inputDF, operation, outputPath, parallelism, apiType, saveMode, opts, nonPartitioned, tableName)
      case "delta" =>
        val tableName = s"delta-$scenarioId"
        writeToDelta(inputDF, operation, outputPath, parallelism, saveMode, nonPartitioned, tableName)
      case "parquet" =>
        writeToParquet(inputDF, operation, outputPath, parallelism, saveMode, nonPartitioned)
      case "iceberg" =>
        val tableName = genIcebergTableName(scenarioId)
        writeToIceberg(inputDF, operation, outputPath, parallelism, saveMode, nonPartitioned, tableName)
      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported")
    }

    val timeTaken = System.currentTimeMillis() - startMs

    println(s"Took ${timeTaken} ms.")

    timeTaken
  }

  private def writeToIceberg(df: DataFrame,
                             operation: String,
                             outputPath: String,
                             parallelism: Int,
                             saveMode: SaveMode,
                             nonPartitioned: Boolean,
                             tableName: String): Unit = {
    val escapedTableName = escapeTableName(tableName)

    withPersisted(df) {
      // NOTE: Iceberg requires incoming dataset to be partitioned, in case it's being ingested
      //       into partitioned table
      val repartitionedDF = df

      /*if (nonPartitioned) {
        df.repartition(parallelism)
      } else {
        df
      }*/

      repartitionedDF.createOrReplaceTempView(s"source")

      operation match {
        case OPERATION_INSERT =>
          // NOTE: Iceberg requires ordering of the dataset when being inserted into partitioned tables
          val insertIntoTableSql =
            s"""
               |INSERT INTO $escapedTableName
               |SELECT * FROM source ${if (nonPartitioned) "" else ""}
               |""".stripMargin

          executeSparkSql(spark, insertIntoTableSql)

        case OPERATION_UPSERT =>
          df.createOrReplaceTempView(s"source")
          // Execute MERGE INTO performing
          //   - Updates for all records w/ matching (partition, key) tuples
          //   - Inserts for all remaining records
          executeSparkSql(spark,
            s"""
               |MERGE INTO $escapedTableName t
               |USING (SELECT * FROM source s)
               |ON t.key = s.key AND t.partition = s.partition
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)

      }
    }
  }

  private def writeToDelta(df: DataFrame,
                           operation: String,
                           outputPath: String,
                           parallelism: Int,
                           saveMode: SaveMode,
                           nonPartitioned: Boolean,
                           tableName: String): Unit = {
    val targetPath = s"$outputPath/$tableName"
    operation match {
      case OPERATION_INSERT =>
        val repartitionedDF = if (nonPartitioned) {
          df.repartition(parallelism)
        } else {
          df.repartition(parallelism, col("partition"))
        }

        val writer = repartitionedDF.write.format("delta")
        val partitionedWriter = if (nonPartitioned) {
          writer
        } else {
          writer.partitionBy("partition")
        }

        partitionedWriter
          .mode(saveMode)
          .save(targetPath)

      case OPERATION_UPSERT =>
        if (!DeltaTable.isDeltaTable(targetPath)) {
          throw new UnsupportedOperationException("Operation 'upsert' is not supported for Delta")
        } else {
          val deltaTable = DeltaTable.forPath(targetPath)
          deltaTable.as("oldData")
            .merge(
              df.as("newData"),
              "oldData.key = newData.key AND oldData.partition = newData.partition")
            .whenMatched
            .updateAll()
            .whenNotMatched
            .insertAll()
            .execute()
        }
    }
  }

  private def writeToParquet(df: DataFrame, operation: String, outputPath: String, parallelism: Int, saveMode: SaveMode, nonPartitioned: Boolean): Unit = {
    operation match {
      case OPERATION_INSERT =>
        val repartitionedDF = if (nonPartitioned) {
          df.repartition(parallelism)
        } else {
          df.repartition(parallelism, col("partition"))
        }

        repartitionedDF.write
          .format("parquet")
          .mode(saveMode)
          .save(s"$outputPath/parquet")

      case OPERATION_UPSERT =>
        throw new UnsupportedOperationException("Operation 'upsert' is not supported for Parquet")
    }
  }

  private def writeToHudi(df: DataFrame,
                          operation: String,
                          outputPath: String,
                          parallelism: Int,
                          apiType: String,
                          saveMode: SaveMode,
                          opts: Map[String, String],
                          nonPartitioned: Boolean,
                          tableName: String): Unit = {
    // TODO cleanup
    val repartitionedDF = if (nonPartitioned) {
      df.repartition(parallelism)
    } else {
      df.repartition(parallelism, col("partition"))
    }

    apiType match {
      case SPARK_DATASOURCE_API =>
        val targetOpts = opts ++ Map(
          HoodieWriteConfig.TABLE_NAME -> "hudi",
        )

        repartitionedDF.write.format("hudi")
          .options(targetOpts)
          .option(DataSourceWriteOptions.OPERATION.key, operation)
          .mode(saveMode)
          .save(s"$outputPath/$tableName")

      case SPARK_SQL_API =>
        repartitionedDF.createOrReplaceTempView("source")

        val escapedTableName = escapeTableName(tableName)

        operation match {
          case OPERATION_INSERT =>
            val insertIntoTableSql =
              s"""
                 |INSERT INTO $escapedTableName
                 |SELECT * FROM source
                 |""".stripMargin

            executeSparkSql(spark, insertIntoTableSql)

          case OPERATION_UPSERT =>
            // Execute MERGE INTO performing
            //   - Updates for all records w/ matching (partition, key) tuples
            //   - Inserts for all remaining records
            executeSparkSql(spark,
              s"""
                 |MERGE INTO $escapedTableName t
                 |USING (SELECT * FROM source s)
                 |ON s.key = t.key AND s.partition = t.partition
                 |WHEN MATCHED THEN UPDATE SET *
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)

        }
    }
  }

  private def escapeTableName(tableName: String) =
    tableName.split('.').map(np => s"`$np`").mkString(".")

  private def genIcebergTableName(scenarioId: String): String =
    s"default.iceberg_$scenarioId"

  private def genHudiTableName(scenarioId: String): String =
    s"default.hudi-$scenarioId".replace("-", "_")
}

object DataLoader {

  val OPERATION_INSERT = "insert"
  val OPERATION_UPSERT = "upsert"
  val SPARK_DATASOURCE_API = "spark-datasource"
  val SPARK_SQL_API = "spark-sql"

  def withPersisted[T](df: DataFrame)(block: => T): T = {
    df.persist(StorageLevel.MEMORY_ONLY)
    try {
      block
    } finally {
      df.unpersist()
    }
  }
}


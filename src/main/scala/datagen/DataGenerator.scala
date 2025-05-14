package datagen

import datagen.DataGenerator.KeyTypes.KeyType
import datagen.DataGenerator.{KeyTypes, expectedCompressionRatio, genParallelRDD, getZipfRecordsPerPartition, lineSepBold, makeCDF, sampleFromCDF}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CatalystUtil.partitionLocalLimit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.Serializable
import java.time.LocalDate
import java.util.UUID
import java.util.UUID.randomUUID
import scala.util.Random

/**
 * Class that can generate workload, based on advanced criteria like insert vs update ratios, spread across
 * partitions.
 *
 * @param spark     Spark's session
 * @param numRounds number of runs of workload generation and the measured operation
 */
class DataGenerator(val spark: SparkSession, val numRounds: Int = 3) extends Serializable {

  private val SEED: Long = 378294793957830L
  private val random = new Random(SEED)

  private val TEXT_VALUE: String = "abcdefghijklmnopqrstuvwxyz"

  private val INPUT_PATH: String = "file:///tmp/hudi-benchmark/input"
  private val OUTPUT_PATH: String = "file:///tmp/hudi-benchmark/output"
  private val HUDI_SOURCE: String = "org.apache.hudi"
  private val INPUT_SOURCE_FORMAT: String = "parquet"
  private final val CITY_NAMES: Array[String] = Array(
    "New York City", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "Indianapolis", "San Francisco", "Seattle", "Denver", "Washington D.C.",
    "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City",
    "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
    "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento",
    "Kansas City", "Atlanta", "Miami", "Colorado Springs", "Raleigh",
    "Omaha", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis",
    "Tulsa", "Arlington", "Tampa", "New Orleans", "Wichita",
    "Cleveland", "Bakersfield", "Aurora", "Anaheim", "Honolulu",
    "Santa Ana", "Riverside", "Corpus Christi", "Lexington", "Stockton",
    "Henderson", "St. Paul", "St. Louis", "Cincinnati", "Pittsburgh",
    "Greensboro", "Anchorage", "Plano", "Lincoln", "Orlando",
    "Irvine", "Newark", "Durham", "Chula Vista", "Toledo",
    "Fort Wayne", "St. Petersburg", "Laredo", "Jersey City", "Chandler",
    "Madison", "Lubbock", "Scottsdale", "Reno", "Buffalo",
    "Gilbert", "Glendale", "North Las Vegas", "Winston-Salem", "Chesapeake",
    "Norfolk", "Fremont", "Garland", "Irving", "Hialeah",
    "Richmond", "Boise", "Spokane", "Baton Rouge", "Tacoma"
  )
  private final val COUNTRY_NAMES: Array[String] = Array(
    "United States", "China", "India", "Brazil", "Russia",
    "Japan", "Germany", "United Kingdom", "France", "Italy",
    "Canada", "South Korea", "Australia", "Spain", "Mexico",
    "Indonesia", "Netherlands", "Saudi Arabia", "Turkey", "Switzerland",
    "Poland", "Sweden", "Belgium", "Thailand", "Ireland",
    "Argentina", "Norway", "Austria", "Nigeria", "Israel",
    "South Africa", "Singapore", "Malaysia", "Egypt", "Colombia",
    "Denmark", "Philippines", "Pakistan", "Chile", "Finland",
    "Portugal", "Vietnam", "Czech Republic", "Romania", "New Zealand",
    "Peru", "Greece", "Hungary", "Kazakhstan", "Ukraine",
    "Algeria", "Qatar", "Morocco", "Kuwait", "Ecuador",
    "Slovakia", "Kenya", "Ethiopia", "Ghana", "Puerto Rico",
    "Oman", "Venezuela", "Guatemala", "Dominican Republic", "Lithuania",
    "Uruguay", "Costa Rica", "Luxembourg", "Panama", "Bulgaria",
    "Croatia", "Lebanon", "Slovenia", "Tunisia", "Jordan",
    "Bahrain", "Latvia", "Estonia", "Cyprus", "Iceland",
    "El Salvador", "Trinidad and Tobago", "Bolivia", "Paraguay", "Cambodia",
    "Nepal", "Cameroon", "Honduras", "Myanmar", "Senegal",
    "Uzbekistan", "Azerbaijan", "Tanzania", "Serbia", "Georgia",
    "Uganda", "Ivory Coast", "Mongolia", "Belarus", "Albania"
  )
  private final val CITY_INDEX_MAP: Map[String, Int] = CITY_NAMES.zipWithIndex.toMap

  import spark.implicits._

  private def getSchema(numFields: Int = 11): StructType = {
    val fields = Seq(
      StructField("key", StringType, nullable = false),
      StructField("partition", StringType, nullable = false),
      StructField("round", IntegerType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("ts2", LongType, nullable = false),
      StructField("city", StringType, nullable = true),
      StructField("country", StringType, nullable = true),
    ) ++ (0 until numFields - 7)
      .map(i => {
        i % 10 match {
          case 0 => StructField(s"textField1$i", StringType, nullable = true)
          case 1 => StructField(s"textField2$i", StringType, nullable = true)
          case 2 => StructField(s"textField3$i", StringType, nullable = true)
          case 3 => StructField(s"textField4$i", StringType, nullable = true)
          case 4 => StructField(s"textField5$i", StringType, nullable = true)
          case 5 => StructField(s"longField1$i", LongType, nullable = true)
          case 6 => StructField(s"decimalField$i", FloatType, nullable = true)
          case 7 => StructField(s"longField2$i", LongType, nullable = true)
          case 8 => StructField(s"longField3$i", LongType, nullable = true)
          case 9 => StructField(s"intField1$i", IntegerType, nullable = true)
        }
      })
    StructType(fields)
  }

  private def newRecord(round: Int,
                        size: Int,
                        partitionPaths: List[String],
                        partitionDistributionCDF: List[Double],
                        keyType: KeyType,
                        schema: StructType) = {
    val ts = System.currentTimeMillis()
    // To induce (semi-) ordering on the keys we simply prefix its random part (UUID) w/ a ts;
    //
    // NOTE: That even though round is preceding the timestamp, it's a constant value for all
    //       records in a batch and therefore doesn't affect the ordering
    val key = keyType match {
      case KeyTypes.TemporallyOrdered =>
        s"${ts}-${randomUUID()}-${"%03d".format(round)}"
      case KeyTypes.Random =>
        s"${randomUUID()}-${"%03d".format(round)}"
      case _ => throw new UnsupportedOperationException(s"$keyType not supported")
    }

    val sizeFactor: Int = Math.max(size / schema.fields.size, 1)
    val fieldValues = schema.fields.map(
      field => {
        field.name match {
          case "key" => key
          case "partition" => partitionPaths(sampleFromCDF(partitionDistributionCDF, random.nextDouble()))
          case "round" => round
          case "ts" => ts
          case "ts2" => ts
          case "city" => CITY_NAMES(random.nextInt(CITY_NAMES.length))
          case "country" => COUNTRY_NAMES(random.nextInt(COUNTRY_NAMES.length))
          case randomField => if (randomField.startsWith("textField")) {
            generateRandomString(sizeFactor + random.nextInt(sizeFactor))
          } else if (randomField.startsWith("decimalField")) {
            random.nextFloat()
          } else if (randomField.startsWith("longField")) {
            random.nextLong()
          } else if (randomField.startsWith("intField")) {
            random.nextInt()
          } else if (randomField.startsWith("arrayField")) {
            (0 until size / sizeFactor / 5).toArray
          } else if (randomField.startsWith("mapField")) {
            (0 until size / sizeFactor / 2 / 40).map(_ => (randomUUID(), random.nextInt())).toMap
          } else {
            TEXT_VALUE
          }
        }
      })

    Row.fromSeq(fieldValues)
  }

  def generateRandomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "!@#$%^&*()-_=+[]{};:,.<>/?".toSeq
    val r = new scala.util.Random()
    (1 to length).map(_ => chars(r.nextInt(chars.length))).mkString
  }

  def randomSubstring(text: String): String = {
    val r = new scala.util.Random()
    val startPos = r.nextInt(text.length)
    val length = r.nextInt(text.length - startPos + 1) + 1 // At least 1 character
    text.substring(startPos, startPos + length)
  }

  /**
   * Pre-generates the workload ahead of time, for the configured number of rounds.
   *
   * @param path                           path to place generated input local_workloads at
   * @param roundsDistribution             total number of records to generate per round
   * @param recordSize                     size of each record in bytes
   * @param updateRatio                    ratio of updates in the batch (remaining will be inserts)
   * @param partitionDistributionMatrixOpt defines to-be-generated records' distribution across partitions (for every round)
   * @param roundsSamplingRatios           fraction of generated records that should belong to a given prior round
   * @param targetDataFileSize             data file size hint that data generation will aim to produce
   * @param skipIfExists                   should skip generation for the rounds possibly generated during previous
   *                                       runs (false by default)
   */
  def generateWorkload(path: String = INPUT_PATH,
                       roundsDistribution: List[Int] = List.fill(numRounds)(1000000),
                       numFields: Int = 11,
                       recordSize: Int = 1024,
                       updateRatio: Double = 0.5f,
                       totalPartitions: Int = -1,
                       partitionDistributionMatrixOpt: Option[List[List[Double]]] = None,
                       roundsSamplingRatios: List[Double] = List(1.0f),
                       targetDataFileSize: Int = 128 * 1024 * 1024,
                       skipIfExists: Boolean = false,
                       keyType: KeyType = KeyTypes.Random,
                       generateUpdatesFromInsertsOnly: Boolean = true,
                       startRound: Int = 0,
                       randomUpdates: Boolean = true,
                       numPartitionsToUpdate: Int = 20): Unit = {
    assert(roundsSamplingRatios.isEmpty || (roundsSamplingRatios.sum - 1.0) < 1e-5)
    assert(totalPartitions != -1 || partitionDistributionMatrixOpt.isDefined)
    if (!generateUpdatesFromInsertsOnly) {
      assert(roundsSamplingRatios.size == 1)
    }

    // Compute records distribution matrix across partitions; such matrix
    // could be explicitly provided as an optional parameter prescribing corresponding
    // distribution for every round
    val (targetPartitionsCount, computedPartitionDistMatrix) =
      genPartitionsDistributionMatrix(totalPartitions, partitionDistributionMatrixOpt)

    val partitionPaths = genDateBasedPartitionValues(targetPartitionsCount)
    val schema = getSchema(numFields)

    ////////////////////////////////////////
    // Generating workload
    ////////////////////////////////////////

    (startRound until numRounds).foreach(curRound => {
      val targetLocation = s"$path/$curRound"
      val partitionDistribution = computedPartitionDistMatrix(curRound)
      // Compute CDF for corresponding records distribution across partitions (for subsequent sampling)
      val partitionDistributionCDF = makeCDF(partitionDistribution)

      val targetLocationPath = new Path(targetLocation)
      val fs = targetLocationPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      if (skipIfExists && fs.exists(targetLocationPath) && fs.listFiles(targetLocationPath, false).hasNext) {
        println(s"Skipping generation for round # $curRound, location $targetLocation is not empty")
      } else {
        // Calculate inserts/updates split
        val targetRecords = roundsDistribution(curRound)
        val numUpdates = if (curRound == 0) 0 else Math.min((updateRatio * targetRecords).toLong, curRound * targetRecords)
        val numInserts = targetRecords - numUpdates

        val targetParallelism = Math.max(2, (targetRecords / (targetDataFileSize / (recordSize * expectedCompressionRatio))).toInt)

        println(
          s"""
             |$lineSepBold
             |Round # $curRound: numInserts $numInserts, numUpdates $numUpdates
             |Creating at $targetLocation
             |$lineSepBold
             |""".stripMargin)

        ////////////////////////////////////////
        // Generating updates
        ////////////////////////////////////////
        // First, we generate updates by sampling records from previous rounds as
        // specified by [[roundsSpread]]
        val rawUpdatesDF = if (randomUpdates) {
          genRandomUpdates(roundsSamplingRatios, curRound, numUpdates, updateRatio, path, generateUpdatesFromInsertsOnly)
        } else {
          getZipfRandomUpdates(partitionPaths, numUpdates, numPartitionsToUpdate, path, curRound)
        }
        val changeCityName = udf((cityName: String) => CITY_NAMES((CITY_INDEX_MAP(cityName) + 1) % CITY_NAMES.length))
        val newTs = System.currentTimeMillis()

        var finalUpdatedDf = rawUpdatesDF
          .withColumn("ts", lit(newTs))
          .withColumn("round", lit(curRound))
        finalUpdatedDf = if (curRound % 3 == 1) {
          finalUpdatedDf.withColumn("city", changeCityName(col("city")))
        } else {
          finalUpdatedDf
        }

        // NOTE: Applying this limit does not guarantee that exactly N elements will be contained in the
        //       returned dataset, since it might not be applying Spark's [[GlobalLimit]] operator.
        //       Instead, it might return slightly higher number of the records (but no more than O(number of partitions)),
        //       since we're simply applying [[LocalLimit]] to circumvent the performance implications of
        //       [[GlobalLimit]] for very large datasets (coalescing all partitions into a single one, then doing
        //       a limit on it)
        val updatesDF = partitionLocalLimit(finalUpdatedDf.repartition(targetParallelism), numUpdates.toInt)

        ////////////////////////////////////////
        // Generating inserts
        ////////////////////////////////////////

        val insertsRDD = genParallelRDD(spark, targetParallelism, 0, numInserts)
          .map(_ => newRecord(curRound, recordSize, partitionPaths, partitionDistributionCDF, keyType, schema))

        val insertsDF = spark.createDataFrame(insertsRDD, schema)

        val upsertDF = if (numUpdates == 0) insertsDF else insertsDF.union(updatesDF)

        spark.time {
          upsertDF
            .repartition(targetParallelism)
            .write
            .format(INPUT_SOURCE_FORMAT)
            .mode(SaveMode.Overwrite)
            .save(targetLocation)
        }
      }
    })
  }

  private def getZipfRandomUpdates(partitionPaths: List[String],
                                   numUpdateRecords: Long,
                                   numPartitionsToWrite: Int,
                                   path: String,
                                   currentRound: Int): DataFrame = {
    val numRecordsPerPartition: List[Int] = getZipfRecordsPerPartition(numUpdateRecords, numPartitionsToWrite)
    val partitionsToUpdate = partitionPaths.take(numPartitionsToWrite)
    println(s"Partitions To Update round # $currentRound: Partitions $partitionsToUpdate")
    println(s"Partitions To Update WITH Counts round # $currentRound: NumRecordsPerPartition $numRecordsPerPartition")

    var sourceDf = spark.read.format(INPUT_SOURCE_FORMAT)
      .load(s"$path/*")
    sourceDf = sourceDf.filter(col("partition").isin(partitionsToUpdate: _*))
    val updateCount = sourceDf.count()
    println(s"Number of updates total records for round # $currentRound: COUNT $updateCount")

    sourceDf.createOrReplaceTempView("source_df_partitions")

    var rankedDF = spark.sql(
      """
        | SELECT key, partition, `round`, rank(key) OVER (PARTITION BY key ORDER BY round DESC) as key_rank
        | FROM source_df_partitions
        |""".stripMargin
    )
    rankedDF = rankedDF.filter($"key_rank" === 1).drop(s"key_rank")
    rankedDF.persist()

    val partitionCounts: Map[String, Long] = rankedDF
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap

    val samplingRatios: Map[String, Double] = partitionCounts.map {
      case (partition, totalRecords) =>
        val desiredCount = numRecordsPerPartition(partitionPaths.indexOf(partition))
        val ratio = 1.0 * desiredCount.toDouble / totalRecords.toDouble
        partition -> ratio
    }
    println(s"SamplingRatios for round # $currentRound: samplingRatios $samplingRatios")

    var fullPlan = spark.emptyDataFrame
    var count: Int = 0
    samplingRatios.foreach(x => {
      val ppf = rankedDF.filter($"partition" === x._1).sample(x._2)
      ppf.persist()
      fullPlan = if (count == 0) {
        count = 1
        ppf
      } else {
        fullPlan.union(ppf)
      }
    })

    // Join sourceDf with fullPlan on key, partition, and round
    val joinCols = Seq("key", "partition", "round")
    val joinedDf = sourceDf.join(fullPlan, joinCols, "inner")
    joinedDf
  }

  private def genRandomUpdates(roundsSamplingRatios: List[Double],
                               curRound: Int,
                               numUpdates: Long,
                               updateRatio: Double,
                               path: String,
                               generateUpdatesFromInsertsOnly: Boolean): DataFrame = {
    val roundsToSample = (Math.max(0, curRound - roundsSamplingRatios.size) until curRound)
    // NOTE: To make sure we always generate appropriate number of updates, we will have to
    //       make sure that our sampling ratios always sum to 1. To make sure this is the case
    //       in case we need to sample from a fewer rounds, we will re-normalize our sampling ratios
    //       to make sure they still sum up to 1.
    val normalizedRoundsSamplingRatios = if (roundsSamplingRatios.size > roundsToSample.size) {
      DataGenerator.normalize(roundsSamplingRatios.take(roundsToSample.size))
    } else {
      roundsSamplingRatios
    }

    roundsToSample
      .map(sourceRound => {
        val sourceRoundRatio = normalizedRoundsSamplingRatios(curRound - sourceRound - 1)
        val updatesFromRound = (numUpdates * sourceRoundRatio).toLong

        println(s"Collecting records to update from prior round # $sourceRound: fraction $sourceRoundRatio, updatesFromRound $updatesFromRound")

        var sourceRoundDF = spark.read.format(INPUT_SOURCE_FORMAT).load(s"$path/$sourceRound")
        sourceRoundDF.printSchema()
        val filteredSourceRoundDF = if (generateUpdatesFromInsertsOnly) {
          sourceRoundDF.filter(s"substr(key, 0, 3) = $sourceRound")
          sourceRoundDF.filter(s"substr(key, length(key) - 2, 3) = '%03d'".format(sourceRound))
        } else {
          sourceRoundDF
        }
        val sourceRoundFilterRatio = 1.0 * filteredSourceRoundDF.count() / sourceRoundDF.count()

        val samplingRatio = updateRatio * sourceRoundRatio
        val adjustedSamplingRatio = samplingRatio / sourceRoundFilterRatio

        val finalDF = filteredSourceRoundDF
          // NOTE: We should not be filtering out records as it will reduce number of updated records we sample
          .sample(adjustedSamplingRatio)
        finalDF
      })
      .foldLeft(spark.emptyDataFrame)((d1, d2) => if (d1.isEmpty) d2 else d1.union(d2))
  }

  private def genDateBasedPartitionValues(targetPartitionsCount: Int): List[String] = {
    // This will generate an ordered sequence of dates in the format of "yyyy/mm/dd"
    // (where most recent one is the first element)
    List.fill(targetPartitionsCount)(LocalDate.now()).zipWithIndex
      .map(t => t._1.minusDays(targetPartitionsCount - t._2))
      .map(d => s"${d.getYear}-${"%02d".format(d.getMonthValue)}-${"%02d".format(d.getDayOfMonth)}")
      .reverse
  }

  private def genPartitionsDistributionMatrix(totalPartitions: Int, partitionDistributionMatrixOpt: Option[List[List[Double]]]) = {
    partitionDistributionMatrixOpt match {
      case Some(partitionDistMatrix) =>
        assert(partitionDistMatrix.size == numRounds)
        partitionDistMatrix.foreach { dist =>
          assert(totalPartitions == -1 || totalPartitions == dist.size, s"$totalPartitions != ${dist.size}")
          assert((dist.sum - 1.0) < 1e-5, s"${dist.sum} != 1.0")
        }

        (partitionDistMatrix.head.size, partitionDistMatrix)

      case None =>
        val dist = List.fill(totalPartitions)(1.0 / totalPartitions)

        (dist.size, List.fill(numRounds)(dist))
    }
  }

  def printWorkloadStats(inputPath: String = INPUT_PATH) = {
    (0 until numRounds).foreach(round => {
      val roundTbl = s"round$round"
      val roundDF = spark.read.format(INPUT_SOURCE_FORMAT).load(s"$inputPath/$round")
      println(s"================Stats for round $round ======================")
      val totalRecords = roundDF.count()
      roundDF.registerTempTable(roundTbl)
      println(s"Total Records: $totalRecords")
      spark.sql(s"select partition, count(*) as cnt, round(1.0 * count(*)/$totalRecords, 2) as pct " +
        s"from $roundTbl group by partition").show(100, false)
      spark.sql(s"select split(key, '-')[0] as round, count(*) as cnt, round(1.0 * count(*)/$totalRecords, 2) as pct " +
        s"from $roundTbl group by round").show(100, false)
      println("========================================================")
    })
  }
}

object DataGenerator {

  private val lineSepBold = "="*50
  private val lineSepLight = "-"*50
  val expectedCompressionRatio = .66

  object KeyTypes extends Enumeration {
    type KeyType = Value
    val Random, TemporallyOrdered = Value
  }

  private def randomUUID(): String =
    UUID.randomUUID().toString

  def sampleFromCDF(cdf: List[Double], weight: Double): Int =
    cdf.indexWhere(d => weight <= d)

  def makeCDF(weights: List[Double]): List[Double] =
    weights.scanLeft(0.0)(_ + _).tail

  private def escapeTableName(tableName: String) =
    tableName.split('.').map(np => s"`$np`").mkString(".")

  private def genParallelRDD(spark: SparkSession, targetParallelism: Int, start: Long, end: Long): RDD[Long] = {
    val partitionSize = (end - start) / targetParallelism
    spark.sparkContext.parallelize(0 to targetParallelism, targetParallelism)
      .mapPartitions { it =>
        val partitionStart = it.next() * partitionSize
        (partitionStart to partitionStart + partitionSize).iterator
      }
  }

  private def withPersisted[T](df: DataFrame)(block: => T): T = {
    df.persist(StorageLevel.MEMORY_ONLY)
    try {
      block
    } finally {
      df.unpersist()
    }
  }

  def genUniformDist(n: Int): List[Double] = {
    normalize(List.fill(n)(1)).toList
  }

  def genExponentialDist(lambda: Double, n: Int): List[Double] = {
    normalize((0 until n).map { i => Math.exp(-lambda * i) }).toList
  }

  def normalize(s: Seq[Double]): Seq[Double] = {
    val sum = s.sum
    s.map(_ / sum)
  }

  private def run(spark: SparkSession, sql: String): Unit = {
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

  // current limitation: partitions are assumed to be continugous.
  // Adjusted shape parameter to meet tail constraint
  private def getZipfRecordsPerPartition(totalRecords: Long, numPartitions: Int, shape: Double = 2.93): List[Int] = {
    // Compute Zipf probabilities
    val ranks = (1 to numPartitions).map(_.toDouble)
    val rawProbs = ranks.map(r => 1.0 / math.pow(r, shape))
    val sumProbs = rawProbs.sum
    val normalizedProbs = rawProbs.map(_ / sumProbs)

    // Calculate record count for each bucket
    val recordsPerBucket = normalizedProbs.map(p => Math.max(1, (p * totalRecords).toInt))

    // Sort from largest to smallest bucket
    val sortedRecords = recordsPerBucket.sorted(Ordering[Int].reverse)
    sortedRecords.toList
  }
}

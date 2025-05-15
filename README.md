# LAKE LOADER

Lake loader is a tool to benchmark incremental load (writes) to data lakes and warehouses. The tool generates input datasets with configurations to cover different aspects of load patterns - number of records, number of partitions, record size, update to insert ratio, distribution of inserts & updates across partitions and total number of rounds of incremental loads to perform. 

The tool consists of two main components:

**Change data generator** This component takes a specified L pattern and generates rounds of inputs. Each input round has change records, which can be either an insert or an update to an insert in a prior input round.

**Incremental Loader** The loader component implements best practices for loading data into various open table formats using popular cloud data platforms like AWS EMR, Databricks and Snowflake. Round 0 is specially designed to perform a one-time bulk load using the preferred bulk loading methods for the data platform. Round 1 and above simply perform incremental loads using pre-generated input change records from each round.

![Figure: Shows the Lake Loader tool's high-level functioning to benchmark incremental loads across popular cloud data platforms.
](src/main/resources/images/lakeLoaderArch.png)


### Building

Lake loader is built with Java 8, 11 or 17 using Maven.

* mvn clean package

### Engine Compatibility

Lake loader requires spark to generate and load the dataset, compatible with spark3.x including spark3.5

### Instructions to generate dataset
In the following sections, we write down the instantiations of the  ChangeDataGenerator class to 
generate datasets that can produce **FACT** tables (Zipfian pattern for updates), **DIM** tables (random pattern for updates) and **EVENTS** table (Append-only table).

**NOTE** Instructions provided are for Spark-Shell, but Spark-Submit can be used with the application class name ChangeDataGenerator. 
#### Fact table arguments
The following generates dateset for a 1TB **FACT** table with 
- 365 partitions based on date
- 1B records
- 30 column schema
- record size of almost 1KB
- 25% of an incremental batch is updates, 75% are new records.
- updates are distributed across partitions in a zipf distribution, with most updates in the latest partitions.
- updates are spread across last 90 partitions, while insert records are written to last 2 partitions with equal spread.

```
import ai.onehouse.lakeloader.ChangeDataGenerator

val input_path = "file:///<input_path>"
val numRounds = 20

## Ensures that the initial load writes to all partitions but the incremental loads only write to latest 2 partitions.
val partitionDistribution:List[List[Double]] = List(List.fill(365)(1.0/365)) ++ List.fill(numRounds-1)(List(0.5, 0.5) ++ List.fill(363)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
                         roundsDistribution = List(1000000000L) ++ List.fill(numRounds-1)(10000000L),
                         numFields = 40,
                         recordSize = 1000,
                         updateRatio = 0.25f,
                         roundsSamplingRatios = List.fill(1)(1f),
                         totalPartitions = 365,
                         partitionDistributionMatrixOpt = Some(partitionDistribution),
                         updatePatterns=UpdatePatterns.Zipf,
                         numPartitionsToUpdate=90)
)
```

#### Dim table arguments
The following generates dateset for a 100GB **DIM** table with
- No partitions or un-partitioned table
- 100M records
- 99 column schema
- record size of almost 1KB
- 50% of an incremental batch is updates, 50% are new records.
- updates are randomly distributed across the entire dataset.

```
import ai.onehouse.lakeloader.ChangeDataGenerator

val input_path = "file:///<input_path>"
val numRounds = 20

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
                         roundsDistribution = List(100000000L) ++ List.fill(numRounds-1)(500000L),
                         numFields = 99,
                         recordSize = 1000,
                         updateRatio = 0.5f,
                         roundsSamplingRatios = List.fill(20)(0.05f),
                         totalPartitions = 1,
                         updatePatterns=UpdatePatterns.Random)
```

#### Events table arguments
The following generates dateset for a 2TB **EVENTS** table with
- 1095 partitions based on date
- 10B records
- 30 column schema
- record size of almost 200Bytes
- 100% of incremental batch are new records (No updates).
- All records in an incremental batch will be generated for the latest partition.

```
import ai.onehouse.lakeloader.ChangeDataGenerator

val input_path = "file:///<input_path>"
val numRounds = 20

## Ensures that the initial load writes to all partitions but the incremental loads only write to latest partition.
val partitionDistribution:List[List[Double]] = List(List.fill(numPartitions)(1.0/numPartitions)) ++ List.fill(numRounds-1)(List(1.0) ++ List.fill(numPartitions-1)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
                         roundsDistribution = List(10000000000L) ++ List.fill(numRounds-1)(50000000L),
                         numFields = 30,
                         recordSize = 200,
                         updateRatio = 0.0f,
                         totalPartitions = 1095,
                         updatePatterns=UpdatePatterns.Random)
```

### Instructions to load data for Apache Hudi

### Instructions to load data for Apache Iceberg

### Instructions to load data for Delta
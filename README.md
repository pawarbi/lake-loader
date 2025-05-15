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


### Instructions to load data for Apache Hudi

### Instructions to load data for Apache Iceberg

### Instructions to load data for Delta
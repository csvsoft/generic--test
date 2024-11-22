package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import scala.io.Source
import java.nio.file.Paths
import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.JavaConverters._
import java.sql.{Connection, Statement}
import java.io.{File, PrintWriter}

object SparkApp extends LazyLogging {
  
  /**
   * Extracts view name from file path by taking the filename without extension
   * @param path Path to the CSV file
   * @return View name derived from filename
   */
  private def getViewNameFromPath(path: String): String = {
    val fileName = Paths.get(path).getFileName.toString
    fileName.lastIndexOf('.') match {
      case -1 => fileName  // No extension
      case i => fileName.substring(0, i)  // Remove extension
    }
  }

  /**
   * Converts a string data type to Spark SQL DataType
   * @param dataType String representation of data type
   * @return Corresponding Spark SQL DataType
   */
  private def stringToDataType(dataType: String): DataType = {
    dataType.trim.toLowerCase match {
      case "string" | "varchar" | "char" | "text" => StringType
      case "int" | "integer" => IntegerType
      case "long" | "bigint" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" | "bool" => BooleanType
      case "date" => DateType
      case "timestamp" => TimestampType
      case "decimal" => DecimalType(38, 18)  // Default precision and scale
      case _ => StringType  // Default to StringType for unknown types
    }
  }

  /**
   * Creates a schema from two header lines (field names and data types)
   * @param path Path to the CSV file
   * @param delimiter Delimiter used in the CSV file
   * @return StructType representing the schema
   */
  private def createSchemaFromHeaders(path: String, delimiter: String): StructType = {
    val source = Source.fromFile(path)
    try {
      val lines = source.getLines().take(2).toArray
      if (lines.length < 2) {
        throw new IllegalArgumentException("CSV file must have at least two lines for headers and data types")
      }

      val fieldNames = lines(0).split(delimiter).map(_.trim)
      val dataTypes = lines(1).split(delimiter).map(_.trim)

      if (fieldNames.length != dataTypes.length) {
        throw new IllegalArgumentException("Number of field names does not match number of data types")
      }

      StructType(
        fieldNames.zip(dataTypes).map { case (fieldName, dataType) =>
          StructField(fieldName, stringToDataType(dataType), true)
        }
      )
    } finally {
      source.close()
    }
  }

  /**
   * Loads a CSV file with two header lines into a DataFrame and creates a view
   * @param spark SparkSession instance
   * @param path Path to the CSV file
   * @param delimiter Delimiter used in the CSV file (default is comma)
   * @param viewName Optional custom view name (default is filename without extension)
   * @return DataFrame containing the CSV data
   */
  def loadCsvFile(
    spark: SparkSession,
    path: String,
    delimiter: String = ",",
    viewName: Option[String] = None
  ): DataFrame = {
    logger.info(s"Loading CSV file from: $path")
    
    // Create schema from the two header lines
    val schema = createSchemaFromHeaders(path, delimiter)
    logger.info(s"Created schema from headers: ${schema.treeString}")
    
    // Read the CSV file, skipping the two header lines
    val df = spark.read
      .option("delimiter", delimiter)
      .option("header", false)
      .schema(schema)
      .csv(path)
      .drop("_c0", "_c1")  // Drop the two header lines that were read as data
    
    // Create or replace view
    val actualViewName = viewName.getOrElse(getViewNameFromPath(path))
    df.createOrReplaceTempView(actualViewName)
    logger.info(s"Created temporary view: $actualViewName")
    
    logger.info(s"Successfully loaded CSV file with ${df.columns.length} columns")
    df
  }

  case class CassandraConfig(
    host: String,
    port: Int = 9042,
    keyspace: String,
    username: Option[String] = None,
    password: Option[String] = None
  )

  case class H2Config(
    url: String = "jdbc:h2:./h2db",
    username: String = "sa",
    password: String = ""
  )

  /**
   * Gets partition key columns for a Cassandra table
   * @param connector CassandraConnector instance
   * @param keyspace Keyspace name
   * @param tableName Table name
   * @return Set of partition key column names
   */
  private def getPartitionKeyColumns(
    connector: CassandraConnector,
    keyspace: String,
    tableName: String
  ): Set[String] = {
    connector.withSessionDo { session =>
      val keyspaceMetadata = session.getMetadata.getKeyspace(keyspace).get
      val tableMetadata = keyspaceMetadata.getTable(tableName).get
      
      // Get partition key columns
      tableMetadata.getPartitionKey.asScala.map(_.getName.toString).toSet
    }
  }

  /**
   * Creates indices in H2 for the specified columns
   * @param h2Config H2 database configuration
   * @param tableName Table name
   * @param columns Columns to create indices for
   */
  private def createIndices(
    h2Config: H2Config,
    tableName: String,
    columns: Set[String]
  ): Unit = {
    // Load H2 JDBC driver
    Class.forName("org.h2.Driver")
    
    var conn: Connection = null
    var stmt: Statement = null
    
    try {
      conn = java.sql.DriverManager.getConnection(
        h2Config.url,
        h2Config.username,
        h2Config.password
      )
      stmt = conn.createStatement()
      
      // Create index for each partition key column
      columns.foreach { column =>
        val indexName = s"idx_${tableName}_${column}"
        val createIndexSql = s"CREATE INDEX IF NOT EXISTS $indexName ON $tableName ($column)"
        
        logger.info(s"Creating index: $createIndexSql")
        stmt.execute(createIndexSql)
        logger.info(s"Successfully created index $indexName")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error creating indices in H2: ${e.getMessage}", e)
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  /**
   * Transfers data from a Cassandra table to an H2 database table and creates indices for partition keys
   * @param spark SparkSession instance
   * @param cassandraConfig Cassandra connection configuration
   * @param h2Config H2 database connection configuration
   * @param tableName Name of the table (same name will be used for both source and target)
   * @param batchSize Number of rows to insert in each batch (default 1000)
   * @return Number of rows transferred
   */
  def transferCassandraToH2(
    spark: SparkSession,
    cassandraConfig: CassandraConfig,
    h2Config: H2Config,
    tableName: String,
    batchSize: Int = 1000
  ): Long = {
    logger.info(s"Starting transfer of table $tableName from Cassandra to H2")

    // Configure Spark for Cassandra connection
    spark.conf.set("spark.cassandra.connection.host", cassandraConfig.host)
    spark.conf.set("spark.cassandra.connection.port", cassandraConfig.port.toString)
    
    // Set credentials if provided
    cassandraConfig.username.foreach(username => 
      spark.conf.set("spark.cassandra.auth.username", username)
    )
    cassandraConfig.password.foreach(password => 
      spark.conf.set("spark.cassandra.auth.password", password)
    )

    try {
      // Create Cassandra connector
      val connector = CassandraConnector(spark.sparkContext.getConf)
      
      // Get partition key columns
      val partitionKeys = getPartitionKeyColumns(connector, cassandraConfig.keyspace, tableName)
      logger.info(s"Found partition keys for table $tableName: ${partitionKeys.mkString(", ")}")

      // Read from Cassandra
      logger.info(s"Reading data from Cassandra table ${cassandraConfig.keyspace}.$tableName")
      val df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "table" -> tableName,
          "keyspace" -> cassandraConfig.keyspace
        ))
        .load()

      // Get the row count for logging
      val rowCount = df.count()
      logger.info(s"Read $rowCount rows from Cassandra")

      // Write to H2
      logger.info(s"Writing data to H2 table $tableName")
      df.write
        .mode(SaveMode.Overwrite)
        .option("driver", "org.h2.Driver")
        .option("batchsize", batchSize)
        .jdbc(
          h2Config.url,
          tableName,
          new java.util.Properties() {
            put("user", h2Config.username)
            put("password", h2Config.password)
          }
        )

      // Create indices for partition keys
      if (partitionKeys.nonEmpty) {
        logger.info(s"Creating indices for partition keys: ${partitionKeys.mkString(", ")}")
        createIndices(h2Config, tableName, partitionKeys)
      }

      logger.info(s"Successfully transferred $rowCount rows to H2 table $tableName")
      rowCount
    } catch {
      case e: Exception =>
        logger.error(s"Error transferring data from Cassandra to H2: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Converts Spark SQL DataType to string representation
   * @param dataType Spark SQL DataType
   * @return String representation of the data type
   */
  private def dataTypeToString(dataType: DataType): String = {
    dataType match {
      case StringType => "string"
      case IntegerType => "int"
      case LongType => "long"
      case FloatType => "float"
      case DoubleType => "double"
      case BooleanType => "boolean"
      case DateType => "date"
      case TimestampType => "timestamp"
      case d: DecimalType => s"decimal(${d.precision},${d.scale})"
      case _ => "string"  // Default to string for unknown types
    }
  }

  /**
   * Exports H2 query results to a CSV file with two-line headers
   * @param spark SparkSession instance
   * @param h2Config H2 database configuration
   * @param query SQL query to execute
   * @param outputPath Path where the CSV file will be written
   * @param delimiter Delimiter to use in the CSV file (default is comma)
   * @param singleFile Whether to write output as a single file (default is true)
   */
  def exportH2QueryToCsv(
    spark: SparkSession,
    h2Config: H2Config,
    query: String,
    outputPath: String,
    delimiter: String = ",",
    singleFile: Boolean = true
  ): Unit = {
    logger.info(s"Executing query and exporting results to CSV: $outputPath")
    logger.info(s"Query: $query")

    try {
      // Read data from H2 using the provided query
      val df = spark.read
        .format("jdbc")
        .option("driver", "org.h2.Driver")
        .option("url", h2Config.url)
        .option("user", h2Config.username)
        .option("password", h2Config.password)
        .option("query", query)
        .load()

      // Get schema information
      val fieldNames = df.schema.fields.map(_.name)
      val dataTypes = df.schema.fields.map(f => dataTypeToString(f.dataType))

      // Create headers
      val headerLine = fieldNames.mkString(delimiter)
      val typeHeaderLine = dataTypes.mkString(delimiter)

      // Ensure output directory exists
      val outputDir = new File(outputPath).getParent
      if (outputDir != null) {
        new File(outputDir).mkdirs()
      }

      // Write headers to a temporary file
      val headerFile = new File(outputPath + ".headers")
      val headerWriter = new PrintWriter(headerFile)
      try {
        headerWriter.println(headerLine)
        headerWriter.println(typeHeaderLine)
      } finally {
        headerWriter.close()
      }

      // Write data
      if (singleFile) {
        // Write as a single file
        df.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("header", false)
          .option("delimiter", delimiter)
          .csv(outputPath + ".temp")

        // Get the data file
        val dataFile = new File(outputPath + ".temp")
          .listFiles()
          .find(_.getName.endsWith(".csv"))
          .getOrElse(throw new RuntimeException("No CSV file found in output"))

        // Combine headers and data
        val finalFile = new File(outputPath)
        val writer = new PrintWriter(finalFile)
        try {
          // Write headers
          writer.println(headerLine)
          writer.println(typeHeaderLine)
          
          // Write data
          Source.fromFile(dataFile).getLines().foreach(writer.println)
        } finally {
          writer.close()
        }

        // Clean up temporary files
        new File(outputPath + ".temp").delete()
        headerFile.delete()
      } else {
        // Write as multiple files with headers in each file
        df.mapPartitions(rows => {
          Iterator(headerLine, typeHeaderLine) ++ rows.map(_.mkString(delimiter))
        }).write
          .mode(SaveMode.Overwrite)
          .text(outputPath)
      }

      val rowCount = df.count()
      logger.info(s"Successfully exported $rowCount rows to $outputPath")
      logger.info(s"Headers written: \n$headerLine\n$typeHeaderLine")
    } catch {
      case e: Exception =>
        logger.error(s"Error exporting query results to CSV: ${e.getMessage}", e)
        throw e
    }
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("Spark Generic Test")
      .master("local[*]") // Use all available cores in local mode
      .getOrCreate()

    try {
      logger.info("Spark Application Started!")

      // Example: Create a simple dataset
      import spark.implicits._
      val data = Seq(1, 2, 3, 4, 5)
      val df = data.toDF("numbers")
      df.show()

      // Example usage of Cassandra to H2 transfer
      // val cassandraConfig = CassandraConfig(
      //   host = "localhost",
      //   keyspace = "mykeyspace",
      //   username = Some("user"),
      //   password = Some("pass")
      // )
      // 
      // val h2Config = H2Config(
      //   url = "jdbc:h2:./mydb",
      //   username = "sa",
      //   password = "password"
      // )
      // 
      // val rowsTransferred = transferCassandraToH2(
      //   spark,
      //   cassandraConfig,
      //   h2Config,
      //   "mytable"
      // )
      // println(s"Transferred $rowsTransferred rows")

      // Example usage of H2 to CSV export
      // val h2Config = H2Config(
      //   url = "jdbc:h2:./mydb",
      //   username = "sa",
      //   password = "password"
      // )
      // 
      // exportH2QueryToCsv(
      //   spark,
      //   h2Config,
      //   "SELECT * FROM employees WHERE department = 'IT'",
      //   "path/to/output.csv"
      // )

      logger.info("Shutting down Spark Application")
    } finally {
      spark.stop()
    }
  }
}

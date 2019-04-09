import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._



object Run {

	def main(args: Array[String]): Unit = {
		// Read a table from Kudu

		val spark: SparkSession = {
			SparkSession
				.builder()
				.master("local")
				.appName("spark example")
				.getOrCreate()
		}

		spark.sparkContext.setLogLevel("ERROR")

//		val df = spark.read
//			.options(Map("kudu.master" -> "192.168.61.100:7051", "kudu.table" -> "kudu_table"))
//			.format("kudu").load

		print("t table read:")
		val t = spark.sqlContext.read.options(Map("kudu.master" -> "192.168.61.100:7051","kudu.table" -> "test_table")).kudu
    t.printSchema()
		t.show(10, false)
		print("t table read end")
		print("###########################################")


		val d = spark.read
			.option("inferSchema", "true")
			.option("header", "true")
			.csv("data/test.csv")

		d.printSchema()

		val df = setNullableStateOfColumn(d, "key", false )

		df.printSchema()


		// Query using the Spark API...
		val filteredDF = df.select("id").filter("id >= 5").show()

		// ...or register a temporary table and use SQL
		//df.registerTempTable("kudu_table")
		//val filteredDF = spark.sql("select id from kudu_table where id >= 5").show()

		// Use KuduContext to create, delete, or write to Kudu tables
		val kuduContext = new KuduContext("192.168.61.100:7051", spark.sparkContext)

		// Create a new Kudu table from a dataframe schema
		// NB: No rows from the dataframe are inserted into the table

		kuduContext.deleteTable("test_table")


		kuduContext.createTable(
			"test_table", df.schema, Seq("key"),
			new CreateTableOptions()
				.setNumReplicas(1)
				.addHashPartitions(List("key").asJava, 3))

		// Insert data
		kuduContext.insertRows(df, "test_table")

		// Delete data
		// kuduContext.deleteRows(filteredDF, "test_table")

		// Upsert data
		kuduContext.upsertRows(df, "test_table")

		// Update data
		val alteredDF = df.select("id","key",  "data")
		alteredDF.show()
		// kuduContext.updateRows(filteredRows, "test_table")

		// Data can also be inserted into the Kudu table using the data source, though the methods on
		// KuduContext are preferred
		// NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert
		// in the options map
		// NB: Only mode Append is supported

		/*		df.write
			.options(Map("kudu.master" -> "192.168.61.100:7051", "kudu.table" -> "test_table"))
			.mode("append")
			.save()*/
		df.write.options(Map("kudu.master" -> "192.168.61.100:7051", "kudu.table" -> "test_table")).mode("append").kudu
		df.write.options(Map("kudu.master" -> "192.168.61.100:7051", "kudu.table" -> "test_table")).mode("append").kudu
		df.write.options(Map("kudu.master" -> "192.168.61.100:7051", "kudu.table" -> "test_table")).mode("append").kudu


		// Check for the existence of a Kudu table
		kuduContext.tableExists("another_table")

		// Delete a Kudu table
//		kuduContext.deleteTable("unwanted_table")

	}




	def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = {

		// get schema
		val schema = df.schema
		// modify [[StructField] with name `cn`
		val newSchema = StructType(schema.map {
			case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
			case y: StructField => y
		})
		// apply new schema
		df.sqlContext.createDataFrame( df.rdd, newSchema )
	}
}

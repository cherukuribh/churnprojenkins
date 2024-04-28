import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import java.util.Properties
import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UpdateDB extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .appName("DB updates")
    .master("local[1]")
    .getOrCreate()

  val customersSchemaddl = StructType(Seq(
    StructField("Customer_ID", IntegerType),
    StructField("Name", StringType),
    StructField("Age", IntegerType),
    StructField("Address", StringType),
    StructField("Postcode", StringType),
    StructField("Phone_Number", StringType),
    StructField("Email", StringType),
    StructField("Credit_Score", IntegerType),
    StructField("Tenure", IntegerType),
    StructField("Country", StringType),
    StructField("Gender", StringType),
    StructField("Products_number", IntegerType),
    StructField("Employment_Status", StringType),
    StructField("Estimated_Salary", FloatType)
  ))


  val customers_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "customerstable").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").schema(customersSchemaddl).load()
  import spark.implicits._

  customers_df.show()

  customers_df.createOrReplaceTempView("customers")
  spark.sql("select * from customers").show(30)

  val updateschemaddl = "UCustomer_ID Int,Old_Phone_Number String,New_Phone_Number String"
  var updates_df = spark.read.option("header", "true")
    .schema(updateschemaddl)
    //  .csv(args(0))
    .csv("D:\\spark_code\\untitled\\Project-Input\\update.csv")

  updates_df.show()
  updates_df.createOrReplaceTempView("update")
  spark.sql("select * from update").show()

  customers_df.createOrReplaceTempView("customers")
  updates_df.createOrReplaceTempView("update")

  import spark.implicits._

  // Join customers_df and updates_df based on Customer_ID and UCustomer_ID
  //  val updatedCustomersDF = customers_df.join(updates_df,
  //    customers_df("Customer_ID") === updates_df("UCustomer_ID") &&
  //    customers_df("Phone_Number") === updates_df("Old_Phone_Number"),"left_outer"
  //  ).select(
  //    customers_df("Customer_ID"),
  //    customers_df("Name"),
  //    customers_df("Age"),
  //    customers_df("Address"),
  //    customers_df("Postcode"),
  //    coalesce(updates_df("New_Phone_number"), customers_df("Phone_Number")).alias("Phone_Number"),
  //    customers_df("Email"),
  //    customers_df("Credit_Score"),
  //    customers_df("Tenure"),
  //    customers_df("Country"),
  //    customers_df("Gender"),
  //    customers_df("Products_number"),
  //    customers_df("Employment_Status"),
  //    customers_df("Estimated_Salary")
  //  )

  // Show the updated DataFrame
  //updatedCustomersDF.count()
  //
  //  val sqlQuery =
  //    """
  //      |SELECT grantee,
  //      |       privilege_type
  //      |FROM pg_catalog.pg_table_acl
  //      |WHERE schemaname = 'customersSchemaddl'
  //      |  AND tablename = 'customers_table'
  //      |""".stripMargin
  //  // Execute the SQL query
  //  val resultDF = spark.sql(sqlQuery)
  // val updatedCustomersDF  = spark.sql("update a set a.Phone_Number=b.New_Phone_Number from customers a ,update b where a.Customer_ID=b.UCustomer_ID and a.Phone_Number = b.Old_Phone_Number")
  val updatedCustomersDF = customers_df.join(
      updates_df,
      customers_df("Customer_ID") === updates_df("UCustomer_ID") && customers_df("Phone_Number") === updates_df("Old_Phone_Number"),
      "left_outer"
    )
    .withColumn("Phone_Number", when($"New_Phone_Number".isNotNull, $"New_Phone_Number").otherwise($"Phone_Number"))
    .drop("Old_Phone_Number", "New_Phone_Number", "UCustomer_ID")

  // Show the updated DataFrame
  updatedCustomersDF.show()

  updatedCustomersDF.show(30)
  updatedCustomersDF.printSchema()




//  //
//  val tableName = "customers_table"
//
//  val sqlQuery = s"DROP TABLE IF EXISTS $tableName"
//
//  spark.sql(sqlQuery)
  // Write the updated DataFrame back to PostgreSQL
//  updatedCustomersDF.write
//    .mode(SaveMode.Overwrite)
//    .format("jdbc")
//    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
//    .option("dbtable", "customerstable")
//    .option("driver", "org.postgresql.Driver")
//    .option("user", "consultants")
//    .option("password", "WelcomeItc@2022")
//    .save()

//  updatedCustomersDF.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(1))
//  updatedCustomersDF.write.mode("overwrite").option("header", "true").saveAsTable("ukusmar.customerstable")
//  println("after customers_table in hive")
}

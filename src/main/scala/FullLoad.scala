import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import java.util.Properties

object FullLoad {
  def main(args: Array[String]): Unit = {

    println("Hello world!")
   // Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("Data_cleaning")
      .enableHiveSupport()
      .getOrCreate()

    // accounts table Dataframe Schema
    val accountSchemaddl = "Account_ID Long,Customer_ID Int,Account_Type String,Balance String," +
      "Open_Date String,Last_Activity_Date String,Credit_Card String,Churn String"

    // loading dataset
    var accounts_df = spark.read.option("header", "true")
      .schema(accountSchemaddl).csv(args(0))

    accounts_df.show()
    accounts_df.printSchema()
    accounts_df.count()
    accounts_df = accounts_df.withColumn("Open_Date", to_date(col("Open_Date"), "dd/MM/yyyy"))
      .withColumn("Last_Activity_Date", to_date(col("Last_Activity_Date"), "dd/MM/yyyy"))

    accounts_df.filter(accounts_df.columns.map(col(_).isNull).reduce(_ || _)).show()

    accounts_df.groupBy("Credit_Card").count().show()

    // cleaning Credit card column
    accounts_df = accounts_df.withColumn("Credit_Card",
      when(upper(col("Credit_Card")).isin("Y", "YES"), "Yes")
        .when(upper(col("Credit_Card")).isin("N", "NO"), "No")
        .otherwise("Unknown")
    )

    accounts_df.groupBy("Credit_Card").count().show()

    // Show DataFrame schema and contents

    //Remove pound sign in balance column if present in the value as its a double(balance in the account)
    // Replace the special character in the Balance column and convert it to FloatType
    val Accounts_cleaned_df = accounts_df.withColumn("Balance", regexp_replace(col("Balance"), "[^0-9.]", ""))
      .withColumn("Balance", col("Balance").cast(FloatType))
    Accounts_cleaned_df.printSchema()
    Accounts_cleaned_df.show()

    // Define the schema to include all columns
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

    // Load the dataset with the defined schema
    var customers_df = spark.read.option("header", "true")
      .schema(customersSchemaddl)
      .csv(args(1))

    customers_df.filter(customers_df.columns.map(col(_).isNull).reduce(_ || _)).show()

    // Show DataFrame schema and contents
    customers_df.show()
    customers_df.printSchema()
    customers_df.groupBy("Gender").count().show()
    customers_df.filter(customers_df.columns.map(col(_).isNull).reduce(_ || _)).show()

    customers_df = customers_df.withColumn("Gender",
      when(upper(col("Gender")).isin("M", "MALE"), "Male")
        .when(upper(col("Gender")).isin("F", "FEMALE"), "Female")
        .otherwise("Unknown"))
    customers_df.groupBy("Gender").count().show()

    val meanAge = customers_df.select(avg("Age")).head().getDouble(0).round.toInt

    // Replace null values in the "Age" column with the mean age
    customers_df = customers_df.withColumn("Age", when(col("Age").isNull, meanAge).otherwise(col("Age")))
    val duplicateRecords = customers_df.groupBy(customers_df.columns.map(col): _*).count().filter(col("count") > 1)
    duplicateRecords.show()
    customers_df = customers_df.dropDuplicates()
    customers_df.groupBy("Employment_Status").count().show()

    customers_df = customers_df.na.fill("Unknown", Seq("Employment_Status"))
    val customers_cleaned_df = customers_df.withColumn("Employment_Status", regexp_replace(col("Employment_Status"), "-", ""))
    customers_cleaned_df.show()
    customers_cleaned_df.groupBy("Employment_Status").count().show()

    // transactions table

    val transactionSchema = "Transaction_ID Long, Account_ID Long, Transaction_Type String, Amount String, Transaction_Date String"
    var transactionsdf = spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(args(2))
    //.csv("D:\\spark_code\\untitled\\Project-Input\\Transactions_data.csv")

    transactionsdf.filter(transactionsdf.columns.map(col(_).isNull).reduce(_ || _)).show()

    transactionsdf.show()
    transactionsdf.groupBy("Transaction_Type").count().show()

    transactionsdf = transactionsdf.withColumn("Transaction_Date", to_date(col("Transaction_Date"), "dd/MM/yyyy"))
    transactionsdf = transactionsdf.withColumn("Amount", regexp_replace(col("Amount"), "[^0-9.]", ""))
      .withColumn("Amount", col("Amount").cast(FloatType))
    var transaction_cleaned_df = transactionsdf.withColumn("Transaction_Type", regexp_replace(col("Transaction_Type"), "-", ""))
      .withColumn("Transaction_Type", regexp_replace(col("Transaction_Type"), " ", ""))
    transaction_cleaned_df.groupBy("Transaction_Type").count().show()

    transaction_cleaned_df.printSchema()
    transaction_cleaned_df.show()
    Accounts_cleaned_df.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "accounts_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").save()
    customers_cleaned_df.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "customers_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").save()
    transaction_cleaned_df.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "transactions_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").save()
    Accounts_cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(3))
    customers_cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(4))
    transaction_cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(5))

    Accounts_cleaned_df.write.mode("overwrite").format("csv").option("header", "true").saveAsTable("ukusmar.accounts_table")
    println("after acocunt_table in hive")
    customers_cleaned_df.write.mode("overwrite").format("csv").option("header", "true").saveAsTable("ukusmar.customers_table")
    println("after customers_table in hive")
    transaction_cleaned_df.write.mode("overwrite").format("csv").option("header", "true").saveAsTable("ukusmar.transactions_table")
    println("after transaction_table in hive ")
  }
}

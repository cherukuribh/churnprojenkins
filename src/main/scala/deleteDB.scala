import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import java.util.Properties

object deleteDB extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .appName("DB deletes")
   // .master("local[1]")
   .enableHiveSupport()
    .getOrCreate()

  var accounts_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "accountstable").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  var customers_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "customerstable").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  var transactions_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "transactionstable").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  // spark SQL

  import spark.implicits._

  accounts_df.createOrReplaceTempView("accounts")
  spark.sql("select * from accounts").show(30)
  customers_df.createOrReplaceTempView("customers")
  spark.sql("select * from customers").show(30)
  transactions_df.createOrReplaceTempView("transactions")
  spark.sql("select * from transactions").show(30)

  val deleteschemaddl = "Account_ID Int"
  var deletes_df = spark.read.option("header", "true")
    .schema(deleteschemaddl)
      .csv(args(0))
    //.csv("D:\\spark_code\\untitled\\Project-Input\\delete.csv")

  deletes_df.show()
  deletes_df.createOrReplaceTempView("delete")
  spark.sql("select * from delete").show()


  // Loop through each row in the deletes DataFrame
  deletes_df.collect().foreach { row =>
    val accountId = row.getAs[Int]("Account_ID")

    // Retrieve the corresponding Customer ID from the accounts table
    val customerIdDF = spark.sql(s"SELECT Customer_ID FROM accounts WHERE Account_ID = $accountId")

    // Extract the Customer ID value from the DataFrame
    val customerId = customerIdDF.first().getInt(0)

    val condition = s"Customer_ID = $customerId"

    // Delete corresponding records from the accounts and customers tables
    accounts_df = accounts_df.filter(!$"Customer_ID".isin(customerId))
    customers_df = customers_df.filter(!$"Customer_ID".isin(customerId))

    transactions_df = transactions_df.filter(!$"Account_ID".isin(accountId))
  }
    accounts_df.show(30)
    customers_df.show(30)
    transactions_df.show(30)

    accounts_df.write.format("jdbc").option("url","jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable","accounts_table").option("driver","org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
    customers_df.write.format("jdbc").option("url","jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable","customers_table").option("driver","org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
    transactions_df.write.format("jdbc").option("url","jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable","transactions_table").option("driver","org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()


  accounts_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(1))
  customers_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(2))
  transactions_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args(3))

  accounts_df.write.mode("overwrite").option("header", "true").saveAsTable("ukusmar.accountstable")
  println("after account_table in hive")
  customers_df.write.mode("overwrite").option("header", "true").saveAsTable("ukusmar.customerstable")
  println("after customers_table in hive")
  transactions_df.write.mode("overwrite").option("header", "true").saveAsTable("ukusmar.transactionstable")
  println("after transaction_table in hive ")

  //////////////////
  ////////////////////////////
  import java.sql.{Connection, DriverManager, Statement}

  // JDBC connection parameters
  val jdbcUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
  val username = "consultants"
  val password = "WelcomeItc@2022"

  // Create a JDBC connection
  var connection: Connection = null
  try {
    connection = DriverManager.getConnection(jdbcUrl, username, password)
    val statement = connection.createStatement()

    // Replace "your_table_name" with the name of the table you want to drop
    val tableName1 = "customerstable"

    // Drop the table
    val sql1 = s"DROP TABLE IF EXISTS $tableName1"
    statement.executeUpdate(sql1)

    println(s"Table $tableName1 dropped successfully.")
    // Rename customers_table to customerstable
    val renameSql1 = "ALTER TABLE customers_table RENAME TO customerstable"
    statement.executeUpdate(renameSql1)
    println("Table customers_table renamed to customerstable successfully.")

    ////////
    val tableName2 = "accountstable"

    // Drop the table
    val sql2 = s"DROP TABLE IF EXISTS $tableName2"
    statement.executeUpdate(sql2)

    println(s"Table $tableName2 dropped successfully.")
    // Rename accounts_table to accountstable
    val renameSql2 = "ALTER TABLE accounts_table RENAME TO accountstable"
    statement.executeUpdate(renameSql2)
    println("Table accounts_table renamed to accountstable successfully.")
    /////////////
    val tableName = "transactionstable"

    // Drop the table
    val sql = s"DROP TABLE IF EXISTS $tableName"
    statement.executeUpdate(sql)

    println(s"Table $tableName dropped successfully.")
    // Rename transactions_table to transactionstable
    val renameSql = "ALTER TABLE transactions_table RENAME TO transactionstable"
    statement.executeUpdate(renameSql)
    println("Table transactions_table renamed to transactionstable successfully.")
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    if (connection != null) {
      connection.close()
    }
  }
///////////////////////






}


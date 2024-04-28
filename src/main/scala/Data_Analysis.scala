import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import java.util.Properties

object Data_Analysis extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .appName("Data_Analysis")
    .master("local[1]")
    .getOrCreate()

  var accounts_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "accounts_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  val customers_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "customers_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  val transactions_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    .option("dbtable", "transactions_table").option("driver", "org.postgresql.Driver").option("user", "consultants")
    .option("password", "WelcomeItc@2022").load()
  // spark SQL

  import spark.implicits._

  accounts_df.createOrReplaceTempView("accounts")
  spark.sql("select * from accounts").show(5)
  customers_df.createOrReplaceTempView("customers")
  spark.sql("select * from customers").show(5)
  transactions_df.createOrReplaceTempView("transactions")
  spark.sql("select * from transactions").show(5)


  // churn count
  val df1 = spark.sql(
    """
      SELECT
      |    CASE
      |        WHEN Balance >= 0 AND Balance <= 1000 THEN '0 - 1000'
      |        WHEN Balance > 1000 AND Balance <= 5000 THEN '1000 - 5000'
      |        WHEN Balance > 5000 AND Balance <= 10000 THEN '5000 - 10000'
      |        ELSE 'Above 10000'
      |    END AS Balance_Range,
      |    Churn,
      |    COUNT(*) AS Count
      |FROM
      |    accounts
      |GROUP BY
      |    CASE
      |        WHEN Balance >= 0 AND Balance <= 1000 THEN '0 - 1000'
      |        WHEN Balance > 1000 AND Balance <= 5000 THEN '1000 - 5000'
      |        WHEN Balance > 5000 AND Balance <= 10000 THEN '5000 - 10000'
      |        ELSE 'Above 10000'
      |    END,
      |    Churn
      |ORDER BY
      |    Balance_Range, Churn
  """.stripMargin
  )
  df1.show()

  val df2 = spark.sql(
    """
      SELECT
      |    CASE
      |        WHEN c.Age >= 18 AND c.Age <= 30 THEN '18-30'
      |        WHEN c.Age > 30 AND c.Age <= 40 THEN '31-40'
      |        WHEN c.Age > 40 AND c.Age <= 50 THEN '41-50'
      |        WHEN c.Age > 50 AND c.Age <= 60 THEN '51-60'
      |        ELSE '61+'
      |    END AS Age_Range,
      |    c.Gender,
      |    a.Churn,
      |    COUNT(*) AS Count
      |FROM
      |    customers c
      |JOIN
      |    accounts a ON c.Customer_ID = a.Customer_ID
      |GROUP BY
      |    CASE
      |        WHEN c.Age >= 18 AND c.Age <= 30 THEN '18-30'
      |        WHEN c.Age > 30 AND c.Age <= 40 THEN '31-40'
      |        WHEN c.Age > 40 AND c.Age <= 50 THEN '41-50'
      |        WHEN c.Age > 50 AND c.Age <= 60 THEN '51-60'
      |        ELSE '61+'
      |    END,
      |    c.Gender,
      |    a.Churn
      |ORDER BY
      |    Age_Range, c.Gender, a.Churn
  """.stripMargin
  )
  df2.show()
  val df3 = spark.sql(
    """
      |SELECT churn, COUNT(*) AS count
      |FROM accounts
      |GROUP BY churn
  """.stripMargin
  )
  df3.show()

  val df4 = spark.sql(
    """
        SELECT c.gender, a.churn, COUNT(*) AS count
      |FROM customers c
      |JOIN accounts a ON c.Customer_ID = a.Customer_ID
      |GROUP BY c.gender, a.churn
      |ORDER BY c.gender, a.churn
  """.stripMargin
  )
  df4.show()

  accounts_df = accounts_df.withColumn("churn",
    when($"Churn" === "Yes", 1)
      .otherwise(0)
  )

  spark.sql(
    """
  SELECT
    c.country,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN a.churn = 'Yes' THEN 1 ELSE 0 END) AS churn_count
FROM
    customers c
JOIN
    accounts a ON c.Customer_ID = a.Customer_ID
GROUP BY
    c.country
ORDER BY
    c.country
    """
  ).show()

  // Perform a join with the customers DataFrame on Account_ID
  val newaccountsdf = accounts_df.join(customers_df, "Customer_ID")
  var newDF = newaccountsdf.join(transactions_df, "Account_ID")


  // Show the result
  newDF.show()// Show the result
  val resultDF = newDF.groupBy("Transaction_Type", "Churn")
    .agg(count("*").alias("transaction_count"))

  resultDF.show()

  //The number of years for which the customer has been with the bank.
  newDF.groupBy("tenure", "churn").count().orderBy("tenure").show()

  // Analyse estimated_salary
  // Calculate min and max salary
  val minSalary = newDF.select(min("estimated_salary")).first().getFloat(0)
  val maxSalary = newDF.select(max("estimated_salary")).first().getFloat(0)

  // Define the range interval
  val salaryInterval = 1000

  // Create a DataFrame with salary ranges
  val salaryRangesDF = spark.range((minSalary / salaryInterval).toInt, (maxSalary / salaryInterval).toInt + 2)
    .selectExpr(
      s"${salaryInterval} * id as min_salary",
      s"(${salaryInterval} * (id + 1)) - 1 as max_salary"
    )
    .withColumn("salary_range", concat(col("min_salary"), lit("-"), col("max_salary")))

  // Bucketize estimated_salary into salary ranges
  val salaryBucketizedDF = newDF.join(salaryRangesDF,
    newDF("estimated_salary").between(salaryRangesDF("min_salary"), salaryRangesDF("max_salary")),
    "left"
  ).drop("min_salary", "max_salary")

  // Group data by salary range and compute churn insights
  salaryBucketizedDF.groupBy("salary_range").agg(
    sum("churn").alias("churn_count"),
    count("churn").alias("total_customers")
  ).orderBy("salary_range").show()
  // credit_score column
  //  // Calculate min and max credit score
  val minCreditScore = newDF.select(min("credit_score")).first().getInt(0)
  val maxCreditScore = newDF.select(max("credit_score")).first().getInt(0)

  // Define the credit score range interval
  val creditScoreInterval = 100

  // Create DataFrame with credit score ranges
  val creditScoreRangesDF = spark.range((minCreditScore.toDouble / creditScoreInterval).toInt,
      (maxCreditScore.toDouble / creditScoreInterval).toInt + 2)
    .selectExpr(
      s"${creditScoreInterval} * id as min_credit_score",
      s"(${creditScoreInterval} * (id + 1)) - 1 as max_credit_score"
    ).withColumn("credit_score_range", concat(col("min_credit_score"), lit("-"), col("max_credit_score")))

  // Bucketize credit score into ranges
  val creditScoreBucketizedDF = newDF.join(creditScoreRangesDF,
    newDF("credit_score").between(creditScoreRangesDF("min_credit_score"), creditScoreRangesDF("max_credit_score")),
    "left"
  ).drop("min_credit_score", "max_credit_score")

  // Group data by credit score range and compute churn insights
  creditScoreBucketizedDF.groupBy("credit_score_range").agg(
    count("churn").alias("total_customers"),
    sum("churn").alias("churn_count")
  ).orderBy("credit_score_range").show()
  // Perform a join with the customers DataFrame on Account_ID
  var joinedDF = accounts_df.join(customers_df, "Customer_ID")

  joinedDF = joinedDF.join(transactions_df, "Account_ID")

  // Select the required columns
  val selectedDF = joinedDF.select("Account_Type", "Churn")

  // Group by Account_Type and Churn, and then count
  selectedDF.groupBy("Account_Type", "Churn")
    .agg(count("*").alias("count")).show()


}

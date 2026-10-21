package com.ecommerce.analytics

import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader

class DataIngestion(spark: SparkSession) {

  import spark.implicits._   // donne à Spark les "traducteurs" vers nos case classes

  // transactions.csv : schéma écrit à la main (exigé par le sujet)
  private val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("user_id",        StringType, nullable = true),
    StructField("product_id",     StringType, nullable = true),
    StructField("merchant_id",    StringType, nullable = true),
    StructField("amount",         DoubleType, nullable = true),
    StructField("timestamp",      StringType, nullable = true),
    StructField("location",       StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category",       StringType, nullable = true)
  ))

  // users.json : schéma écrit à la main, avec le champ imbriqué preferred_categories
  private val userSchema: StructType = StructType(Seq(
    StructField("user_id",              StringType,             nullable = true),
    StructField("age",                  IntegerType,            nullable = true),
    StructField("annual_income",        DoubleType,             nullable = true),
    StructField("city",                 StringType,             nullable = true),
    StructField("customer_segment",     StringType,             nullable = true),
    StructField("preferred_categories", ArrayType(StringType),  nullable = true),
    StructField("registration_date",    StringType,             nullable = true)
  ))

  def readTransactions(): Dataset[Transaction] =
    spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(ConfigLoader.transactionsPath)
      .as[Transaction]

  def readUsers(): Dataset[User] =
    spark.read
      .schema(userSchema)
      .json(ConfigLoader.usersPath)       // une ligne du fichier = un objet JSON
      .as[User]

  def readProducts(): Dataset[Product] =
    spark.read
      .parquet(ConfigLoader.productsPath) // on donne le DOSSIER products.parquet
      .as[Product]

  def readMerchants(): Dataset[Merchant] =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")     // Spark devine les types (exigé par le sujet)
      .csv(ConfigLoader.merchantsPath)
      .withColumn("establishment_date", col("establishment_date").cast(StringType))
      .as[Merchant]

  // Question 2.3 : lecture protégée par un try-catch
  def safeLoad[T](datasetName: String)(read: => Dataset[T]): Option[Dataset[T]] =
    try {
      val ds = read
      val nbRead = ds.count()   // action : oblige Spark à lire vraiment le fichier
      println(s"[$datasetName] lignes lues avant validation : $nbRead")
      Some(ds)
    } catch {
      case e: AnalysisException =>
        println(s"[$datasetName] ERREUR : fichier introuvable ou structure incorrecte. ${e.getMessage}")
        None
      case e: Exception =>
        println(s"[$datasetName] ERREUR pendant la lecture : ${e.getMessage}")
        None
    }
}
package com.ecommerce.analytics
// Propriétaire : FMC (membre A). Fichier de test, à supprimer avant le rendu

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader

object TestFMC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestFMC")
      .master(ConfigLoader.sparkMaster)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   // moins de lignes rouges dans la console

    try {
      val ingestion  = new DataIngestion(spark)
      val validation = new DataValidation(spark)

      ingestion.safeLoad("transactions")(ingestion.readTransactions()).foreach { ds =>
        ds.printSchema()
        ds.show(5)
        val (ok, ko) = validation.validateTransactions(ds)
        ko.groupBy("rejection_reason").count().show(false)
      }

      ingestion.safeLoad("users")(ingestion.readUsers()).foreach { ds =>
        ds.printSchema()
        ds.show(5, truncate = false)
        val (ok, ko) = validation.validateUsers(ds)
        ko.groupBy("rejection_reason").count().show(false)
      }

      ingestion.safeLoad("products")(ingestion.readProducts()).foreach { ds =>
        ds.printSchema()
        val (ok, ko) = validation.validateProducts(ds)
        ko.groupBy("rejection_reason").count().show(false)
      }

      ingestion.safeLoad("merchants")(ingestion.readMerchants()).foreach { ds =>
        ds.printSchema()
        val (ok, ko) = validation.validateMerchants(ds)
        ko.show(false)
      }
    } finally {
      spark.stop()
    }
  }
}
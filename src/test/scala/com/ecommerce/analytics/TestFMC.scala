package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader

object TestFMC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestFMC")
      .master(ConfigLoader.sparkMaster)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val ingestion  = new DataIngestion(spark)
      val validation = new DataValidation(spark)

      for {
        tx <- ingestion.safeLoad("transactions")(ingestion.readTransactions())
        us <- ingestion.safeLoad("users")(ingestion.readUsers())
        pr <- ingestion.safeLoad("products")(ingestion.readProducts())
        me <- ingestion.safeLoad("merchants")(ingestion.readMerchants())
      } {
        val (txOk, txKo) = validation.validateTransactions(tx)
        val (usOk, usKo) = validation.validateUsers(us)
        val (prOk, prKo) = validation.validateProducts(pr)
        val (meOk, meKo) = validation.validateMerchants(me)

        val orphans = validation.countOrphans(tx, us, pr, me)
        val report = validation.qualityReport(Seq(
          ("transactions", tx, txOk, txKo),
          ("users",        us, usOk, usKo),
          ("products",     pr, prOk, prKo),
          ("merchants",    me, meOk, meKo)
        ), Some(orphans))
        report.show(false)
        validation.saveQualityReport(report)

        validation.saveRejected("transactions", txKo)
        validation.saveRejected("users", usKo)
        validation.saveRejected("products", prKo)
        validation.saveRejected("merchants", meKo)
        println(s"Rapport et rejets écrits dans ${ConfigLoader.outputPath}")
      }
    } finally {
      spark.stop()
    }
  }
}
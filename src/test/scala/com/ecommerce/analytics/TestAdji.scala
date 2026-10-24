package com.ecommerce.analytics
//Fichier de test, rangé dans src/test/scala (hors du JAR)

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.ecommerce.utils.ConfigLoader

object TestAdji {
  def main(args: Array[String]): Unit = {
    // Session ouverte ici, sans dépendre du fichier SparkSessionBuilder de Bland
    val spark = SparkSession.builder()
      .appName("TestAdji")
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    try {
      // 1. L'UDF face aux cas pièges (question 3.1)
      val pieges = Seq[String](
        null, "", "abc",
        "20240701021822",   // lundi 2h18
        "20240706093000",   // samedi 9h30
        "20241225170000",   // mercredi 17h00 pile
        "20240231120000",   // 31 février : n'existe pas
        "20240101240000",   // 24h00 : n'existe pas
        "20251231225959"    // mercredi 22h59
      ).toDF("timestamp")
      TimeFeatures.addTimeFeatures(pieges).show(false)

      // 2. Toute la partie 3 sur les vraies données (questions 3.2 et 3.3)
      val ingestion  = new DataIngestion(spark)
      val validation = new DataValidation(spark)

      // Le bloc ne s'exécute que si les quatre lectures ont réussi
      for {
        tx <- ingestion.safeLoad("transactions")(ingestion.readTransactions())
        us <- ingestion.safeLoad("users")(ingestion.readUsers())
        pr <- ingestion.safeLoad("products")(ingestion.readProducts())
        me <- ingestion.safeLoad("merchants")(ingestion.readMerchants())
      } {
        val (txOk, _) = validation.validateTransactions(tx)
        val (usOk, _) = validation.validateUsers(us)
        val (prOk, _) = validation.validateProducts(pr)
        val (meOk, _) = validation.validateMerchants(me)

        val transformation = new DataTransformation()
        val enrichi = transformation.enrichTransactionData(txOk, usOk, prOk, meOk)
        val complet = transformation.addWindowFeatures(enrichi).cache()   // gardé en mémoire : on le compte plusieurs fois

        complet.printSchema()
        println(s"Lignes après les jointures : ${complet.count()}")
        complet.groupBy("age_group").count().orderBy("age_group").show()
        println(s"Utilisateurs distincts : ${complet.select("user_id").distinct().count()}")
        println(s"Lignes où le rang égale le total : ${complet.filter(col("user_tx_rank") === col("user_tx_count")).count()}")
        println(s"Lignes is_active_user = 1 : ${complet.filter(col("is_active_user") === 1).count()}")
        println(s"Lignes sans achat précédent : ${complet.filter(col("days_since_prev_tx").isNull).count()}")

        // Un client à vérifier à la main
        complet.filter(col("user_id") === "U01474")
          .orderBy("user_tx_rank")
          .select("transaction_id", "timestamp", "amount", "age_group", "user_tx_rank", "user_tx_count",
            "rolling_7d_amount", "is_active_user", "days_since_prev_tx")
          .show(false)
      }
    } finally {
      spark.stop()
    }
  }
}
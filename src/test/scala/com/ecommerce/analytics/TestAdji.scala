package com.ecommerce.analytics
// Propriétaire : Adji (membre B). Fichier de test, rangé dans src/test/scala (hors du JAR)

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.ecommerce.utils.ConfigLoader

object TestAdji {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestAdji")
      .master(ConfigLoader.sparkMaster)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    try {
      // 1. Des cas pièges écrits à la main
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

      // 2. Le vrai fichier, lu tel quel (sans validation, tout en texte)
      val tx      = spark.read.option("header", "true").csv(ConfigLoader.transactionsPath)
      val enrichi = TimeFeatures.addTimeFeatures(tx)
      enrichi.printSchema()
      println(s"Lignes lues : ${enrichi.count()}")
      println(s"Timestamps illisibles : ${enrichi.filter(col("hour").isNull).count()}")
      enrichi.groupBy("day_period").count().orderBy("day_period").show()
    } finally {
      spark.stop()
    }
  }
}
package com.ecommerce.analytics

import org.apache.spark.sql.functions.{abs, col}
import com.ecommerce.utils.SparkSessionBuilder

object TestBland {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.build()
    import spark.implicits._

    try {
      // 1. La session : les trois réglages viennent-ils bien d'application.conf ?
      println(s"Application : ${spark.sparkContext.appName}")
      println(s"Master : ${spark.sparkContext.master}")
      println(s"Partitions de shuffle : ${spark.conf.get("spark.sql.shuffle.partitions")}")

      // 2. Huit transactions écrites à la main, avec les colonnes du contrat d'Adji
      val transactions = Seq(
        ("T01", "U1", "M1", "Boutique Alpha", "Electronics", "Bretagne",  0.10, 100.0, "Jeune"),
        ("T02", "U1", "M1", "Boutique Alpha", "Electronics", "Bretagne",  0.10,  50.0, "Jeune"),
        ("T03", "U2", "M1", "Boutique Alpha", "Electronics", "Bretagne",  0.10, 150.0, "Adulte"),
        ("T04", "U3", "M2", "Librairie Beta", "Books",       "Bretagne",  0.05,  20.0, "Senior"),
        ("T05", "U4", "M2", "Librairie Beta", "Books",       "Bretagne",  0.05,  30.0, "Âge Moyen"),
        ("T06", "U5", "M3", "Tech Gamma",     "Electronics", "Occitanie", 0.20, 200.0, "Inconnu"),
        ("T07", "U2", "M3", "Tech Gamma",     "Electronics", "Occitanie", 0.20, 100.0, "Adulte"),
        ("T08", "U3", "M4", "Livres Delta",   "Books",       "Occitanie", 0.08,  80.0, "Senior")
      ).toDF("transaction_id", "user_id", "merchant_id", "merchant_name", "merchant_category",
        "region", "commission_rate", "amount", "age_group")

      // 3. Le rapport
      val rapport = Analytics.merchantReport(transactions)
      rapport.show(false)
      rapport.printSchema()

      // 4. Contrôle : les cinq tranches additionnées doivent redonner le CA, au centime près
      val sommeTranches = col("ca_jeune") + col("ca_adulte") + col("ca_age_moyen") +
        col("ca_senior") + col("ca_inconnu")
      val ecarts = rapport.filter(abs(sommeTranches - col("chiffre_affaires")) > 0.01)
      println(s"Marchands dont les tranches ne redonnent pas le CA : ${ecarts.count()}")
    } finally {
      spark.stop()
    }
  }
}
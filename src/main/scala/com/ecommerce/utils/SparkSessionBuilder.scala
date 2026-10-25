package com.ecommerce.utils

import org.apache.spark.sql.SparkSession


object SparkSessionBuilder {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions.toString)
      .config("spark.sql.debug.maxToStringFields", "100")   // journal : décrire jusqu'à 100 colonnes

    // spark-submit range son --master dans la propriété système "spark.master".
    // S'il l'a fait, on le respecte. Sinon (lancement depuis IntelliJ), on prend application.conf.
    val masterDejaChoisi = sys.props.contains("spark.master")
    val builderComplet =
      if (masterDejaChoisi) builder
      else builder.master(ConfigLoader.sparkMaster)

    val spark = builderComplet.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
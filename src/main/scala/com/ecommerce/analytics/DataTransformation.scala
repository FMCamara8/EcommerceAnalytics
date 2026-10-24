package com.ecommerce.analytics
// Propriétaire : Adji (membre B)

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, collect_set, count, floor, lag, round, row_number, size, sum, when}
import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader

class DataTransformation {

  private val secondesParJour: Long = 86400L

  // Question 5.2 (Bland) : une petite table est envoyée en entier à chaque cœur,
  // ce qui évite de redistribuer la grosse table des transactions (shuffle).
  // Le réglage enable-broadcast d'application.conf permet de l'éteindre.
  private def petiteTable(df: DataFrame): DataFrame =
    if (ConfigLoader.enableBroadcast) broadcast(df) else df

  // Tranche d'âge (question 3.2). 25 ans compte comme Adulte, un âge absent donne Inconnu.
  private val trancheAge: Column =
    when(col("age").isNull, "Inconnu")
      .when(col("age") < 25, "Jeune")
      .when(col("age") < 45, "Adulte")
      .when(col("age") < 65, "Âge Moyen")
      .otherwise("Senior")

  // Question 3.2 : jointures, UDF, tranche d'âge, rang et nombre de transactions par utilisateur
  def enrichTransactionData(transactions: Dataset[Transaction],
                            users: Dataset[User],
                            products: Dataset[Product],
                            merchants: Dataset[Merchant]): DataFrame = {

    // 1. De chaque table, on ne garde que les colonnes utiles, sous des noms qui ne se marchent pas dessus
    val usersUtiles = users.select("user_id", "age", "customer_segment")

    val productsUtiles = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price"),
      col("rating"))

    val merchantsUtiles = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region"),
      col("commission_rate"))

    // 2. Les trois jointures, à partir des transactions
    val joint = transactions.toDF()
      .join(petiteTable(usersUtiles), Seq("user_id"), "left")              // garde l'achat même sans profil valide
      .join(petiteTable(productsUtiles), Seq("product_id"), "left")        // garde l'achat même sans produit valide
      .join(petiteTable(merchantsUtiles), Seq("merchant_id"), "inner")     // exige un marchand valide (rapport 4.1)

    // 3. L'UDF de la question 3.1, puis la tranche d'âge
    val avecTemps = TimeFeatures.addTimeFeatures(joint)
      .withColumn("age_group", trancheAge)

    // 4. Les deux fenêtres de la question 3.2
    val parUtilisateurDansLOrdre = Window.partitionBy("user_id").orderBy(col("timestamp"), col("transaction_id"))
    val parUtilisateur           = Window.partitionBy("user_id")   // sans orderBy : toute la pile de l'utilisateur

    avecTemps
      .withColumn("user_tx_rank", row_number().over(parUtilisateurDansLOrdre))
      .withColumn("user_tx_count", count("*").over(parUtilisateur))
      .select(
        // les colonnes des transactions
        "transaction_id", "user_id", "product_id", "merchant_id", "amount", "timestamp",
        "location", "payment_method", "category",
        // l'utilisateur
        "age", "age_group", "customer_segment",
        // le produit
        "product_name", "product_category", "price", "rating",
        // le marchand
        "merchant_name", "merchant_category", "region", "commission_rate",
        // l'UDF
        "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours",
        // les fenêtres
        "user_tx_rank", "user_tx_count")
  }

  // Question 3.3 : montant sur 7 jours, utilisateur actif, délai depuis l'achat précédent
  def addWindowFeatures(enriched: DataFrame): DataFrame = {

    // Deux horloges sans fuseau horaire : la seconde et le jour, comptés depuis le 1er janvier 1970
    val avecHorloge = enriched
      .withColumn("tx_seconds", TimeFeatures.extractEpochSeconds(col("timestamp")))
      .withColumn("tx_day", floor(col("tx_seconds") / secondesParJour))

    // Les 7 × 24 heures qui précèdent la transaction, celle-ci comprise
    val septDerniersJours = Window.partitionBy("user_id")
      .orderBy(col("tx_seconds"))
      .rangeBetween(-7 * secondesParJour, 0)

    // Le jour de la transaction et les 6 jours d'avant : 7 jours du calendrier
    val septJoursCalendaires = Window.partitionBy("user_id")
      .orderBy(col("tx_day"))
      .rangeBetween(-6, 0)

    // Le même ordre que user_tx_rank, pour trouver l'achat précédent
    val dansLOrdre = Window.partitionBy("user_id").orderBy(col("timestamp"), col("transaction_id"))

    avecHorloge
      .withColumn("rolling_7d_amount", round(sum("amount").over(septDerniersJours), 2))
      .withColumn("is_active_user",
        when(size(collect_set("tx_day").over(septJoursCalendaires)) >= 5, 1).otherwise(0))
      .withColumn("days_since_prev_tx",
        round((col("tx_seconds") - lag("tx_seconds", 1).over(dansLOrdre)) / secondesParJour.toDouble, 2))
      .drop("tx_seconds", "tx_day")
  }
}
package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, length, lit, round, sum, when}
import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader._   // donne accès direct à minAge, maxAge...

class DataValidation(spark: SparkSession) {

  import spark.implicits._

  // Coupe un tableau étiqueté en deux, affiche les comptes et vérifie qu'aucune ligne ne se perd
  private def split(name: String, read: DataFrame, labelled: DataFrame): (DataFrame, DataFrame) = {
    val valid    = labelled.filter(col("rejection_reason").isNull).drop("rejection_reason")
    val rejected = labelled.filter(col("rejection_reason").isNotNull)

    val nbRead     = read.count()
    val nbValid    = valid.count()
    val nbRejected = rejected.count()
    println(s"[$name] lues : $nbRead | valides : $nbValid | rejetées : $nbRejected")
    if (nbValid + nbRejected != nbRead)
      println(s"[$name] ATTENTION : valides + rejetées ne font pas le total lu, des lignes ont disparu")

    (valid, rejected)
  }

  // Transactions : amount > 0 et timestamp de 14 caractères
  def validateTransactions(ds: Dataset[Transaction]): (Dataset[Transaction], DataFrame) = {
    val labelled = ds.withColumn("rejection_reason",
      when(col("amount").isNull, "amount manquant")
        .when(col("amount") <= minAmount, "amount inférieur ou égal à 0")
        .when(col("timestamp").isNull, "timestamp manquant")
        .when(length(col("timestamp")) =!= timestampLength, "timestamp sans 14 caractères"))
    val (valid, rejected) = split("transactions", ds.toDF(), labelled)
    (valid.as[Transaction], rejected)
  }

  // Users : age entre 16 et 100, annual_income > 0
  def validateUsers(ds: Dataset[User]): (Dataset[User], DataFrame) = {
    val labelled = ds.withColumn("rejection_reason",
      when(col("age").isNull, "age manquant")
        .when(col("age") < minAge || col("age") > maxAge, "age hors de 16 à 100")
        .when(col("annual_income").isNull, "annual_income manquant")
        .when(col("annual_income") <= minIncome, "annual_income inférieur ou égal à 0"))
    val (valid, rejected) = split("users", ds.toDF(), labelled)
    (valid.as[User], rejected)
  }

  // Products : price > 0 et rating entre 1 et 5
  def validateProducts(ds: Dataset[Product]): (Dataset[Product], DataFrame) = {
    val labelled = ds.withColumn("rejection_reason",
      when(col("price").isNull, "price manquant")
        .when(col("price") <= minPrice, "price inférieur ou égal à 0")
        .when(col("rating").isNull, "rating manquant")
        .when(col("rating") < minRating || col("rating") > maxRating, "rating hors de 1 à 5"))
    val (valid, rejected) = split("products", ds.toDF(), labelled)
    (valid.as[Product], rejected)
  }

  // Merchants : commission_rate entre 0 et 1
  def validateMerchants(ds: Dataset[Merchant]): (Dataset[Merchant], DataFrame) = {
    val labelled = ds.withColumn("rejection_reason",
      when(col("commission_rate").isNull, "commission_rate manquant")
        .when(col("commission_rate") < minCommission || col("commission_rate") > maxCommission,
          "commission_rate hors de 0 à 1"))
    val (valid, rejected) = split("merchants", ds.toDF(), labelled)
    (valid.as[Merchant], rejected)
  }

  // ---------- Question 2.4 : rapport de qualité ----------

  // Nombre total de cases vides d'un tableau, toutes colonnes confondues
  private def countNulls(ds: Dataset[_]): Long = {
    val nullsInRow = ds.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
    ds.select(coalesce(sum(nullsInRow), lit(0L))).first().getLong(0)
  }

  // Bonus 2.5 : nombre de transactions dont la référence n'existe pas dans son référentiel
  def countOrphans(transactions: Dataset[_], users: Dataset[_],
                   products: Dataset[_], merchants: Dataset[_]): (Long, Long, Long) = {

    def orphans(key: String, referential: Dataset[_]): Long =
      transactions.select(key)
        .join(referential.select(key), Seq(key), "left_anti")
        .count()

    (orphans("user_id", users), orphans("product_id", products), orphans("merchant_id", merchants))
  }

  // Une ligne par dataset : (nom, lu, valides, rejetées).
  // orphans (bonus 2.5) est facultatif : sans lui, le rapport reste celui de la question 2.4.
  def qualityReport(datasets: Seq[(String, Dataset[_], Dataset[_], DataFrame)],
                    orphans: Option[(Long, Long, Long)] = None): DataFrame = {
    val lines = datasets.map { case (name, read, valid, rejected) =>
      (name, read.count(), valid.count(), rejected.count(), countNulls(read))
    }
    val base = lines
      .toDF("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "nb_valeurs_nulles")
      .withColumn("taux_rejet",
        when(col("nb_lignes_lues") === 0, lit(0.0))
          .otherwise(round(col("nb_lignes_rejetees") * 100.0 / col("nb_lignes_lues"), 2)))
      .select("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees",
        "taux_rejet", "nb_valeurs_nulles")

    orphans match {
      case Some((users, products, merchants)) =>
        val isTransactions = col("dataset") === "transactions"
        base
          .withColumn("nb_user_id_orphelins",     when(isTransactions, lit(users)))
          .withColumn("nb_product_id_orphelins",  when(isTransactions, lit(products)))
          .withColumn("nb_merchant_id_orphelins", when(isTransactions, lit(merchants)))
      case None => base
    }
  }

  // Dossier de sortie + sous-dossier, avec ou sans "/" final dans application.conf
  private def outputDir(sub: String): String = outputPath.stripSuffix("/") + "/" + sub

  // Le rapport, en un seul fichier CSV avec ligne d'en-tête
  def saveQualityReport(report: DataFrame): Unit =
    report.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputDir("rapport_qualite"))

  // Les lignes rejetées d'un dataset, en JSON (garde les listes comme preferred_categories)
  def saveRejected(name: String, rejected: DataFrame): Unit =
    rejected.write.mode("overwrite").json(outputDir(s"rejets/$name"))
}
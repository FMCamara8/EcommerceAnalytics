package com.ecommerce.analytics

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, count_distinct, min, rank, round, substring, sum, when}

object Analytics {

  // CA d'une tranche d'âge : pour chaque ligne, le montant si la tranche correspond, sinon 0,
  // puis la somme.
  private def caTranche(tranche: String): Column =
    round(sum(when(col("age_group") === tranche, col("amount")).otherwise(0.0)), 2)

  // Question 4.1 : une ligne par marchand
  def merchantReport(df: DataFrame): DataFrame = {

    // Bonus 4.1 : seulement si le tableau contient la colonne du bonus 3.4.
    val avecSuspects = df.columns.contains("is_suspicious")

    val comptesSuspects: Seq[Column] =
      if (avecSuspects) Seq(
        sum("is_suspicious").as("nb_transactions_suspectes"),
        round(avg("is_suspicious") * 100, 2).as("taux_suspects"))
      else Seq.empty

    // Temps 1 : une pile par marchand, et des comptes sur chaque pile
    val parMarchand = df
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region", "commission_rate")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        (Seq(
          count("*").as("nb_transactions"),
          count_distinct(col("user_id")).as("nb_clients_uniques"),
          round(avg("amount"), 2).as("montant_moyen"),
          round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale"),
          caTranche("Jeune").as("ca_jeune"),
          caTranche("Adulte").as("ca_adulte"),
          caTranche("Âge Moyen").as("ca_age_moyen"),
          caTranche("Senior").as("ca_senior"),
          caTranche("Inconnu").as("ca_inconnu")
        ) ++ comptesSuspects): _*
      )

    // Temps 2 : les classements, sur ce petit tableau d'une ligne par marchand
    val fenetreCategorie = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val fenetreRegion    = Window.partitionBy("region").orderBy(col("chiffre_affaires").desc)

    val colonnes = Seq(
      "merchant_id", "merchant_name", "merchant_category", "region", "commission_rate",
      "chiffre_affaires", "nb_transactions", "nb_clients_uniques", "montant_moyen", "commission_totale",
      "rang_categorie", "rang_region",
      "ca_jeune", "ca_adulte", "ca_age_moyen", "ca_senior", "ca_inconnu"
    ) ++ (if (avecSuspects) Seq("nb_transactions_suspectes", "taux_suspects") else Seq.empty)

    parMarchand
      .withColumn("rang_categorie", rank().over(fenetreCategorie))
      .withColumn("rang_region", rank().over(fenetreRegion))
      .select(colonnes.map(nom => col(nom)): _*)
      .orderBy(col("chiffre_affaires").desc, col("merchant_id"))
  }

  // Question 4.2 : la matrice de rétention, une ligne par couple (cohort_month, period_index)
  def cohortAnalysis(df: DataFrame): DataFrame = {

    // 1. Le mois de chaque achat : en texte (202403) pour l'affichage,
    //    en numéro (année × 12 + mois) pour pouvoir soustraire deux mois
    val achats = df
      .filter(col("user_id").isNotNull)
      .select(
        col("user_id"),
        col("amount"),
        substring(col("timestamp"), 1, 6).as("tx_month"),
        (substring(col("timestamp"), 1, 4).cast("int") * 12 +
          substring(col("timestamp"), 5, 2).cast("int")).as("tx_month_num")
      )

    // 2. La promo du client = son premier mois d'achat (fenêtre par client, sans ordre)
    val parClient = Window.partitionBy("user_id")
    val avecCohorte = achats
      .withColumn("cohort_month", min("tx_month").over(parClient))
      .withColumn("period_index", col("tx_month_num") - min("tx_month_num").over(parClient))

    // 3. La taille de chaque promo : ses clients actifs au mois 0
    val tailles = avecCohorte
      .filter(col("period_index") === 0)
      .groupBy("cohort_month")
      .agg(count_distinct(col("user_id")).as("nb_utilisateurs_initiaux"))

    // 4. Chaque case de la matrice : clients actifs, CA, puis le taux par rapport à la taille de la promo
    avecCohorte
      .groupBy("cohort_month", "period_index")
      .agg(
        count_distinct(col("user_id")).as("nb_utilisateurs_actifs"),
        sum("amount").as("ca_exact")
      )
      .join(SparkOptimizations.broadcastIfEnabled(tailles), Seq("cohort_month"))
      .withColumn("taux_retention",
        round(col("nb_utilisateurs_actifs") * 100.0 / col("nb_utilisateurs_initiaux"), 2))
      .withColumn("chiffre_affaires", round(col("ca_exact"), 2))
      .withColumn("revenu_moyen_utilisateur", round(col("ca_exact") / col("nb_utilisateurs_actifs"), 2))
      .select("cohort_month", "period_index", "nb_utilisateurs_initiaux", "nb_utilisateurs_actifs",
        "taux_retention", "chiffre_affaires", "revenu_moyen_utilisateur")
      .orderBy("cohort_month", "period_index")
  }

  // Question 4.2, dernier point : la promo qui garde le plus de clients au 3e mois
  def bestCohortAt3Months(retention: DataFrame): DataFrame =
    retention
      .filter(col("period_index") === 3)
      .orderBy(col("taux_retention").desc, col("cohort_month"))
      .limit(1)
}
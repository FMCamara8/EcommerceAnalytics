package com.ecommerce.analytics
// Propriétaire : Bland (membre C). Intégration validée par les trois membres

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}

object MainApp {

  // Dossier de sortie, avec un seul "/" à la fin, quoi qu'écrive application.conf
  private val outputDir: String = ConfigLoader.outputPath.stripSuffix("/") + "/"

  // Un résultat final est écrit deux fois : en CSV (lisible dans Excel) et en Parquet (compact, garde les types)
  private def saveResult(df: DataFrame, name: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"${outputDir}csv/$name")
    df.write.mode(SaveMode.Overwrite).parquet(s"${outputDir}parquet/$name")
    println(s"[$name] écrit dans ${outputDir}csv/$name et ${outputDir}parquet/$name")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.build()

    // Gestion d'erreurs globale : quoi qu'il arrive, la session est fermée
    val succes: Boolean =
      try {
        runPipeline(spark)
      } catch {
        case e: Exception =>
          println(s"ERREUR : le pipeline s'est arrêté. ${e.getClass.getSimpleName} : ${e.getMessage}")
          e.printStackTrace()
          false
      } finally {
        spark.stop()
        println("SparkSession arrêtée.")
      }

    // Code de sortie 1 en cas d'échec, après la fermeture de la session
    if (!succes) sys.exit(1)
  }

  private def runPipeline(spark: SparkSession): Boolean = {
    val ingestion      = new DataIngestion(spark)
    val validation     = new DataValidation(spark)
    val transformation = new DataTransformation()

    // ---------- 1. Ingestion (FMC) ----------
    println("\n=== 1. Ingestion ===")
    val lectures = for {
      tx <- ingestion.safeLoad("transactions")(ingestion.readTransactions())
      us <- ingestion.safeLoad("users")(ingestion.readUsers())
      pr <- ingestion.safeLoad("products")(ingestion.readProducts())
      me <- ingestion.safeLoad("merchants")(ingestion.readMerchants())
    } yield (tx, us, pr, me)

    lectures match {
      case None =>
        println("Au moins un fichier n'a pas pu être lu : arrêt du pipeline.")
        false

      case Some((txLu, usLu, prLu, meLu)) =>
        // Chaque fichier lu sert 4 à 5 fois : on le garde en mémoire (question 5.1)
        val tx = SparkOptimizations.cacheIfEnabled(txLu)
        val us = SparkOptimizations.cacheIfEnabled(usLu)
        val pr = SparkOptimizations.cacheIfEnabled(prLu)
        val me = SparkOptimizations.cacheIfEnabled(meLu)

        // ---------- 2. Validation et rapport de qualité (FMC) ----------
        println("\n=== 2. Validation ===")
        val (txOk, txKo) = validation.validateTransactions(tx)
        val (usOk, usKo) = validation.validateUsers(us)
        val (prOk, prKo) = validation.validateProducts(pr)
        val (meOk, meKo) = validation.validateMerchants(me)

        val orphans = validation.countOrphans(tx, us, pr, me)   // bonus 2.5 (FMC)
        val rapportQualite = validation.qualityReport(Seq(
          ("transactions", tx, txOk, txKo),
          ("users",        us, usOk, usKo),
          ("products",     pr, prOk, prKo),
          ("merchants",    me, meOk, meKo)
        ), Some(orphans))
        rapportQualite.show(false)
        validation.saveQualityReport(rapportQualite)
        validation.saveRejected("transactions", txKo)
        validation.saveRejected("users", usKo)
        validation.saveRejected("products", prKo)
        validation.saveRejected("merchants", meKo)

        // ---------- 3. Transformation (Adji) ----------
        println("\n=== 3. Transformation ===")
        val enrichi = SparkOptimizations.persistLarge(
          transformation.addSuspiciousFlags(                  // bonus 3.4
            transformation.addWindowFeatures(                 // question 3.3
              transformation.enrichTransactionData(txOk, usOk, prOk, meOk))))   // question 3.2
        println(s"Transactions enrichies : ${enrichi.count()} lignes, ${enrichi.columns.length} colonnes")
        enrichi.show(5)

        // Le tableau enrichi est en mémoire : les fichiers lus ne servent plus
        SparkOptimizations.release(tx, us, pr, me)

        // Bonus 3.4 : les transactions suspectes
        println(s"Transactions suspectes : ${enrichi.filter(col("is_suspicious") === 1).count()}")
        enrichi.groupBy("suspicion_score").count().orderBy("suspicion_score").show()
        val topSuspectes = transformation.topSuspicious(enrichi)
        println("Les 20 transactions suspectes au montant le plus élevé :")
        topSuspectes.show(truncate = false)

        // ---------- 4. Analytique (Bland) ----------
        println("\n=== 4. Analytique ===")
        val kpiMarchands = SparkOptimizations.cacheIfEnabled(Analytics.merchantReport(enrichi))
        println(s"KPI par marchand : ${kpiMarchands.count()} marchands")
        kpiMarchands.show(20, truncate = false)

        val cohortes = SparkOptimizations.cacheIfEnabled(Analytics.cohortAnalysis(enrichi))
        println(s"Matrice de rétention : ${cohortes.count()} lignes (cohorte x période)")
        cohortes.show(30, truncate = false)

        val meilleure = Analytics.bestCohortAt3Months(cohortes)
        println("Cohorte avec la meilleure rétention à 3 mois :")
        meilleure.show(truncate = false)

        // ---------- 5. Écriture des résultats ----------
        println("\n=== 5. Écriture ===")
        saveResult(kpiMarchands, "kpi_marchands")
        saveResult(cohortes, "cohortes")
        saveResult(meilleure, "meilleure_cohorte_3_mois")
        saveResult(topSuspectes, "top20_transactions_suspectes")
        enrichi.write.mode(SaveMode.Overwrite).parquet(s"${outputDir}parquet/transactions_enrichies")
        println(s"[transactions_enrichies] écrit dans ${outputDir}parquet/transactions_enrichies")

        SparkOptimizations.release(enrichi, kpiMarchands, cohortes)
        println(s"\nPipeline terminé. Résultats dans $outputDir")
        true
    }
  }
}
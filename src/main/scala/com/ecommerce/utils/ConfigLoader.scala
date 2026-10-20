package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  // Lit application.conf, plus les éventuelles surcharges -Dcle=valeur du lancement
  private val config: Config = ConfigFactory.load()

  // --- Le mécanisme de valeur par défaut exigé par le sujet ---
  // Si la clé existe dans le fichier, on prend sa valeur, sinon la valeur par défaut
  private def getString(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  private def getInt(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  private def getDouble(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default

  private def getBoolean(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default

  // --- Application et Spark ---
  val appName: String         = getString("app.name", "EcommerceAnalytics")
  val sparkMaster: String     = getString("app.spark.master", "local[*]")
  val shufflePartitions: Int  = getInt("app.spark.shuffle.partitions", 8)

  // --- Optimisations ---
  val enableCache: Boolean     = getBoolean("app.optimization.enable-cache", true)
  val enableBroadcast: Boolean = getBoolean("app.optimization.enable-broadcast", true)

  // --- Chemins des données ---
  val transactionsPath: String = getString("app.data.input.transactions", "src/main/resources/data/transactions.csv")
  val usersPath: String        = getString("app.data.input.users", "src/main/resources/data/users.json")
  val productsPath: String     = getString("app.data.input.products", "src/main/resources/data/products.parquet")
  val merchantsPath: String    = getString("app.data.input.merchants", "src/main/resources/data/merchants.csv")
  val outputPath: String       = getString("app.data.output.path", "output/")

  // --- Seuils de validation ---
  val minAmount: Double        = getDouble("app.validation.transaction.min-amount", 0.0)
  val timestampLength: Int     = getInt("app.validation.transaction.timestamp-length", 14)
  val minAge: Int              = getInt("app.validation.user.min-age", 16)
  val maxAge: Int              = getInt("app.validation.user.max-age", 100)
  val minIncome: Double        = getDouble("app.validation.user.min-income", 0.0)
  val minPrice: Double         = getDouble("app.validation.product.min-price", 0.0)
  val minRating: Double        = getDouble("app.validation.product.min-rating", 1.0)
  val maxRating: Double        = getDouble("app.validation.product.max-rating", 5.0)
  val minCommission: Double    = getDouble("app.validation.merchant.min-commission", 0.0)
  val maxCommission: Double    = getDouble("app.validation.merchant.max-commission", 1.0)
}
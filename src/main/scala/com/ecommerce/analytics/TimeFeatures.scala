package com.ecommerce.analytics
// Propriétaire : Adji (membre B)

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

// Ce que l'UDF rend pour un timestamp : une fiche de 6 cases.
// Les noms sont en snake_case parce qu'ils deviendront les noms des colonnes.
case class TimeInfo(
                     hour: Int,              // 0 à 23
                     day_of_week: String,    // "Monday" ... "Sunday"
                     month: String,          // "January" ... "December"
                     is_weekend: Int,        // 1 si samedi ou dimanche, sinon 0
                     day_period: String,     // "Morning", "Afternoon", "Evening" ou "Night"
                     is_working_hours: Int   // 1 si 9 <= heure < 17, sinon 0
                   )

object TimeFeatures {

  // Le moule qui lit un texte comme 20240701021822.
  // uuuu = l'année. STRICT = refuse les dates impossibles (31 février, 24h00...).
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  // L'étiquette de la journée à partir de l'heure
  def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"            // 22h-23h et 0h-5h

  // La logique en Scala pur, sans Spark.
  // Rend Some(fiche) si le texte est lisible, None sinon (null, vide, mal formé).
  def parse(ts: String): Option[TimeInfo] =
    Try(LocalDateTime.parse(ts, formatter)).toOption.map { dt =>
      val hour = dt.getHour
      val day  = dt.getDayOfWeek
      TimeInfo(
        hour             = hour,
        day_of_week      = day.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
        month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
        is_weekend       = if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) 1 else 0,
        day_period       = dayPeriod(hour),
        is_working_hours = if (hour >= 9 && hour < 17) 1 else 0
      )
    }

  // L'UDF : la même logique, emballée pour Spark.
  // None devient null, la façon dont Spark dit « pas de valeur ».
  val extractTimeFeatures: UserDefinedFunction =
    udf((ts: String) => parse(ts).orNull)

  // Applique l'UDF puis déplie la fiche en 6 colonnes simples.
  def addTimeFeatures(df: DataFrame): DataFrame =
    df.withColumn("time_features", extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")
}
// Exigence 1 : la version de Scala avec laquelle Spark 4.2 a été compilé
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0"

// Le numéro donné par "spark-submit --version" sur le PC qui a Spark installé
lazy val sparkVersion = "4.2.0"

// Imposé par le sujet (structure 1.1 et question 6.2)
lazy val mainClassName = "com.ecommerce.analytics.MainApp"

// Java 21 verrouille des pièces internes dont Spark a besoin
lazy val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    // Exigences 2 et 3 : Spark Core, Spark SQL et Typesafe Config
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.typesafe"      % "config"     % "1.4.3"
    ),

    // Pour que "sbt run" fonctionne malgré "provided" (README, exécution locale)
    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated,
    Compile / run / mainClass := Some(mainClassName),
    run / fork := true,
    run / javaOptions ++= sparkJavaOptions,

    // Exigence 4 : le JAR exécutable, fabriqué par sbt-assembly
    assembly / mainClass       := Some(mainClassName),
    assembly / assemblyJarName := "EcommerceAnalytics.jar",
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyMergeStrategy := {
      case "reference.conf"         => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
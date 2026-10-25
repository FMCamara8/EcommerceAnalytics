# EcommerceAnalytics

Pipeline Spark écrit en Scala qui lit les données d'une boutique en ligne (transactions,
utilisateurs, produits, marchands), les nettoie, les relie entre elles et calcule des
indicateurs : rapport de qualité des données, KPI par marchand et analyse de cohortes.
Projet final du module Data Engineer, Spark et Scala, groupe 9.

## Prérequis

| Outil | Version utilisée | Remarque |
| --- | --- | --- |
| Java (JDK) | 21 | `java -version` doit afficher 21 |
| sbt | 1.13.0 | https://www.scala-sbt.org/download |
| Scala | 2.13.18 | téléchargé automatiquement par sbt, rien à installer |
| Spark | 4.2.0, précompilé pour Hadoop 3 | utile seulement pour `spark-submit` |

Installation de Spark : télécharger `spark-4.2.0-bin-hadoop3.tgz` sur
https://spark.apache.org/downloads.html, le décompresser, définir la variable
`SPARK_HOME` sur ce dossier et ajouter `SPARK_HOME/bin` au `PATH`. Vérifier avec :

    spark-submit --version

Sous Windows, Spark a aussi besoin de `winutils.exe` et `hadoop.dll` (Hadoop 3.4) :
les placer dans `C:\hadoop\bin`, définir `HADOOP_HOME=C:\hadoop`, ajouter
`C:\hadoop\bin` au `PATH` et copier `hadoop.dll` dans `C:\Windows\System32`.
Le chemin du projet ne doit contenir ni espace ni accent.

## Structure

    EcommerceAnalytics/
    ├── build.sbt                    dépendances et fabrication du JAR
    ├── project/plugins.sbt          plugin sbt-assembly
    ├── src/main/scala/com/ecommerce/
    │   ├── analytics/               lecture, validation, transformations, analytique, MainApp
    │   ├── models/                  case classes des quatre jeux de données
    │   └── utils/                   ConfigLoader, SparkSessionBuilder
    ├── src/main/resources/
    │   ├── application.conf         configuration
    │   └── data/                    les quatre fichiers d'entrée
    └── src/test/scala/              programmes de test de chaque module

## Configuration

Tous les paramètres sont dans `src/main/resources/application.conf` : chemins d'entrée
et de sortie, seuils de validation, réglages Spark. Une clé absente du fichier prend
une valeur par défaut définie dans `ConfigLoader`. On peut remplacer une valeur pour un seul lancement, sans modifier le
fichier ni recompiler, en la passant comme propriété système Java
(`-Dcle=valeur`). Par exemple, pour écrire les résultats dans `resultats/` :

    spark-submit --driver-java-options "-Dapp.data.output.path=resultats/" --class com.ecommerce.analytics.MainApp --master "local[*]" target/scala-2.13/EcommerceAnalytics.jar

Les chemins des données sont relatifs : lancez toujours les commandes depuis la racine
du projet.

## Compilation

    sbt compile
    sbt clean assembly

La seconde commande produit `target/scala-2.13/EcommerceAnalytics.jar`. Spark n'est pas
inclus dans le JAR (dépendance `provided`) : `spark-submit` le fournit à l'exécution.

## Exécution locale avec sbt

    sbt run

sbt lance `com.ecommerce.analytics.MainApp` dans une JVM séparée, avec Spark et les
options nécessaires à Java 21 (réglées dans `build.sbt`). Le mode d'exécution vient de
`app.spark.master`, par défaut `local[*]` : tous les cœurs de la machine.

## Exécution avec spark-submit

En local, depuis la racine du projet :

    spark-submit --class com.ecommerce.analytics.MainApp --master "local[*]" target/scala-2.13/EcommerceAnalytics.jar

Sur un cluster YARN, les données doivent être sur un stockage partagé (HDFS par exemple),
car les machines du cluster ne voient pas les fichiers de votre PC. On donne alors les
chemins au lancement :

    spark-submit \
      --class com.ecommerce.analytics.MainApp \
      --master yarn \
      --deploy-mode cluster \
      --conf "spark.driver.extraJavaOptions=-Dapp.data.input.transactions=hdfs:///ecommerce/transactions.csv -Dapp.data.input.users=hdfs:///ecommerce/users.json -Dapp.data.input.products=hdfs:///ecommerce/products.parquet -Dapp.data.input.merchants=hdfs:///ecommerce/merchants.csv -Dapp.data.output.path=hdfs:///ecommerce/output/" \
      EcommerceAnalytics.jar

Le `--master` donné à `spark-submit` est prioritaire sur celui d'`application.conf`.

## Résultats

Le dossier de sortie (par défaut `output/`) contient :

- `rapport_qualite/` : le rapport de qualité des données, en CSV ;
- `rejets/<jeu de données>/` : les lignes rejetées par la validation, en JSON, avec la
  colonne `rejection_reason` ;
- `csv/` et `parquet/` : les résultats d'analyse, chacun dans un sous-dossier à son nom,
  aux deux formats : `kpi_marchands`, `cohortes`, `meilleure_cohorte_3_mois`,
  `top20_transactions_suspectes` ;
- `parquet/transactions_enrichies/` : le tableau final de 35 colonnes (Parquet seulement).

## Équipe

La répartition du travail est dans `EQUIPE.md`, le journal de contribution et les
décisions techniques dans `CONTRIBUTIONS.md`.
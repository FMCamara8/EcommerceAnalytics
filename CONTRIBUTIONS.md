# Journal de contribution : Grpupe 9

## Répartition des questions

| Question | Responsable | Relecteur |
| --- | --- | --- |
| 0.1 à 0.3 | Les trois | Les trois |
| 1.1 à 1.3 | Fodé Mamoudou Camara | Blandine Mokam |
| 2.1 à 2.4 | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 3.1 à 3.3 | Adji Ndioba Dia | Fodé Mamoudou Camara |
| 4.1 et 4.2 | Blandine Mokam | Adji Ndioba Dia |
| 5.1 et 5.2 | Blandine Mokam | Fodé Mamoudou Camara et Adji Ndioba Dia |
| 6.1 | Blandine Mokam | Intégration validée par les trois |
| 7.1 | Fodé Mamoudou Camara | Blandine Mokam |

| Question | Responsable | Relecteur |
| --- | --- | --- |
| 0.1 à 0.3 Organisation, Git, journal | les trois | les trois |
| 1.1 Structure SBT | Fodé Mamoudou Camara | Blandine Mokam |
| 1.2 `build.sbt` et JAR | Fodé Mamoudou Camara | Blandine Mokam |
| 1.3 `README.md` | Fodé Mamoudou Camara | Blandine Mokam |
| 2.1 Ingestion multi-format | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 2.2 Validation des données | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 2.3 Gestion d'erreurs et résumé | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 2.4 Rapport de qualité | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 2.5 Intégrité référentielle (bonus) | Fodé Mamoudou Camara | Adji Ndioba Dia |
| 3.1 UDF `extractTimeFeatures` | Adji Ndioba Dia | Fodé Mamoudou Camara |
| 3.2 `enrichTransactionData` | Adji Ndioba Dia | Fodé Mamoudou Camara |
| 3.3 Fonctions de fenêtrage | Adji Ndioba Dia | Fodé Mamoudou Camara |
| 3.4 Transactions suspectes (bonus) | Adji Ndioba Dia | Fodé Mamoudou Camara |
| 4.1 KPI par marchand (et puce bonus) | Blandine Mokam | Adji Ndioba Dia |
| 4.2 Analyse de cohortes (et puce bonus) | Blandine Mokam | Adji Ndioba Dia |
| 5.1 Optimisation du stockage | Blandine Mokam | Fodé Mamoudou Camara et Adji Ndioba Dia |
| 5.2 Optimisation des jointures | Blandine Mokam | Fodé Mamoudou Camara et Adji Ndioba Dia |
| 6.1 `MainApp` | Blandine Mokam | intégration validée par les trois |
| 7.1 `application.conf` | Fodé Mamoudou Camara | Blandine Mokam |
| 8.1 et 8.2 Rendu et soutenance | les trois | les trois |

## Charge de travail et difficultés

### Fodé Mamoudou Camara (membre A) : environ 30 heures

| Étape | Heures |
| --- | --- |
| Installation de l'environnement et premiers pas avec Spark et Scala | 5 |
| Structure du projet, `build.sbt`, plugin sbt-assembly | 4 |
| `application.conf`, `ConfigLoader`, case classes | 5 |
| Ingestion, validation, gestion d'erreurs, rapport de qualité | 9 |
| README et bonus 2.5 | 4 |
| Intégration, JAR, documentation de fin de projet | 3 |

Difficultés rencontrées :

- IntelliJ affichait 23 erreurs et aucun bouton de lancement alors que `sbt compile` réussissait. Il a fallu vider les caches d'IntelliJ (Invalidate Caches) pour qu'il relise le projet.
- `MergeStrategy` introuvable au premier chargement du `build.sbt` : le fichier `project/plugins.sbt` qui déclare sbt-assembly n'était pas encore créé au bon endroit.
- Erreur `UnsatisfiedLinkError` sur `NativeIO$Windows.access0` à la lecture de `products.parquet`, un dossier et non un fichier. Résolue en copiant `hadoop.dll` dans `C:\Windows\System32`.
- Les valeurs nulles : un filtre comme `amount > 0` écarte aussi les lignes vides des deux côtés. La validation passe donc par une étiquette `rejection_reason`, et on vérifie que valides + rejetées = lues.
- Le mode ANSI de Spark 4 fait planter une division par zéro : le taux de rejet est protégé par un `when`.
- Un push refusé sur GitHub (historiques divergents), mal résolu par un push forcé, puis règle de groupe adoptée : `git pull --no-edit` avant chaque `git push`, jamais de push forcé.

### Adji Ndioba Dia (membre B) : environ 24 heures

| Étape | Heures |
| --- | --- |
| Installation de l'environnement et premiers pas avec Spark et Scala | 4 |
| UDF `extractTimeFeatures` et ses tests | 6 |
| Jointures et enrichissement (3.2) | 5 |
| Fonctions de fenêtrage (3.3) | 5 |
| Bonus 3.4 | 4 |

Difficultés rencontrées :

- Le premier `TestAdji` ne compilait pas : il utilisait `SparkSessionBuilder`, un fichier de Bland pas encore récupéré avec `git pull`. Les tests ouvrent depuis leur propre session.
- Les dates : il fallait une lecture stricte (un 31 février doit donner null), des noms de jours et de mois identiques sur tous les PC (`Locale.ENGLISH`) et une horloge sans fuseau horaire pour les fenêtres.
- La fenêtre glissante de 7 jours se calcule sur des secondes avec `rangeBetween`, et non sur un nombre de lignes.
- Le délai de 5 minutes du bonus 3.4 : `days_since_prev_tx`, arrondi en jours, ne permettait pas de le voir. Le délai a été recalculé en secondes.
- La console remplie de lignes rouges INFO au démarrage, réglée par un fichier `log4j2.properties`.


### Blandine Mokam (membre C) : environ 28 heures

| Étape | Heures |
| --- | --- |
| Installation de l'environnement et premiers pas avec Spark et Scala | 4 |
| `SparkSessionBuilder` et KPI par marchand (4.1) | 6 |
| Analyse de cohortes (4.2) | 5 |
| Optimisations (5.1 et 5.2) | 4 |
| `MainApp`, intégration des trois modules, écriture des résultats | 6 |
| Puces bonus de 4.1 et 4.2 | 3 |

Difficultés rencontrées :

- `NoClassDefFoundError: org/apache/spark/sql/SparkSession$` au premier lancement de `MainApp` : la configuration créée par IntelliJ n'avait ni la case « provided » ni les options Java 21.
- 9 marchands valides n'ont pas de région. La validation 2.2 ne contrôle que la commission : ils restent dans le rapport, classés entre eux dans une région vide.
- Les cohortes d'octobre à décembre 2025 n'ont pas encore de troisième mois observé. Elles sont exclues du classement « meilleure rétention à 3 mois ».
- Un avertissement qui coupait la description du plan d'exécution dans le journal, réglé par `spark.sql.debug.maxToStringFields = 100`.
- Brancher les fonctions des deux autres membres dans `MainApp` sans casser leurs tests.


## Décisions techniques du groupe
### Versions de Spark, Scala et Java
- Spark 4.2.0, Scala 2.13.18 et Java 21 sur les trois PC. Spark 4 n'existe qu'en Scala 2.13
  et ses bibliothèques sont compilées avec Scala 2.13.18 : on a pris exactement celle-là.
- Java 21 fait partie des versions supportées par Spark 4.2 et était déjà installé sur un PC.
### Fabrication du JAR : sbt-assembly plutôt que sbt package
- sbt package ne met dans le JAR que notre code. Or le programme a besoin de Typesafe
  Config, que Spark ne fournit pas : spark-submit s'arrêterait sur une ClassNotFoundException.
- sbt-assembly ajoute Typesafe Config. Spark est déclaré en "provided" et Scala est retiré
  de l'assemblage, puisque spark-submit les fournit : le JAR pèse X Mo au lieu de plus de
  200 Mo, et il n'y a jamais deux versions de Spark en même temps.
- Le dossier data de src/main/resources est écarté du JAR (MergeStrategy.discard) : Spark
    lit les données sur le disque, jamais dans le JAR. Le JAR passe ainsi de 3,9 Mo à 0,4 Mo,
    contre plus de 200 Mo s'il contenait Spark.
- La tâche run est reconfigurée dans build.sbt (classpath complet, JVM séparée, options
  --add-opens de Java 21) pour que "sbt run" fonctionne malgré "provided".
### Valeurs par défaut de la configuration
- Toutes les valeurs viennent d'application.conf. Si une clé manque, ConfigLoader prend une
  valeur de secours écrite dans le code (hasPath) plutôt que dans un reference.conf : le
  mécanisme reste visible et aucun fichier n'est ajouté à l'arborescence imposée.
- Une clé présente mais mal écrite (un texte à la place d'un nombre) arrête le programme avec
  une ConfigException, plutôt que de valider les données avec un seuil faux.
### Chemins des données
Les chemins sont relatifs à la racine du projet (src/main/resources/data/...), car Spark
ne peut pas lire un fichier placé à l'intérieur du JAR. Le programme se lance donc depuis
la racine du projet. Chaque chemin peut être remplacé au lancement avec -Dcle=valeur,
sans recompiler.
### Mode ANSI de Spark 4
- Laissé activé, comme Spark 4 le fait par défaut. Aucune conversion risquée n'est faite :
  timestamp reste du texte, et le taux de rejet du rapport est protégé contre la division
  par zéro, qui ferait planter le programme en mode ANSI.
### Types des case classes (question 2.1)
- Champs numériques en Option[Double] et Option[Int] : un Double Scala ne peut pas valoir
  null, une case vide ferait planter le programme. Les textes restent en String.
- timestamp reste du texte, car 136 lignes contiennent "NA".
- preferred_categories en Seq[String] plutôt qu'en Array[String] : Spark accepte les deux,
  mais une Seq s'affiche lisiblement et deux Seq de même contenu sont égales.
- Noms des champs en snake_case, identiques aux colonnes des fichiers.
### Validation des données (question 2.2)
- Chaque ligne reçoit une colonne rejection_reason : la première règle violée, ou null.
  Le découpage valides / rejetées se fait sur isNull / isNotNull, donc aucune ligne ne
  disparaît ; le programme vérifie que valides + rejetées = lues.
- Montant, revenu et prix strictement positifs (0 est rejeté) ; âge, note et commission
  bornes comprises (16 et 100, 1 et 5, 0 et 1).
- Rejets : 1 890 transactions, 345 utilisateurs, 179 produits et 14 marchands.
### Rapport de qualité et lignes rejetées (questions 2.4 et 2.5)
- Valeurs nulles et références orphelines comptées sur les données lues, avant validation :
  le rapport décrit la qualité des fichiers reçus.
- Orphelins comptés par jointure left_anti contre les référentiels complets. Un utilisateur
  rejeté pour son âge existe, il n'est pas orphelin. Résultat : 400 user_id, 300 product_id
  et 250 merchant_id orphelins, sur la seule ligne transactions du rapport.
- Rapport en un seul fichier CSV (coalesce(1)) ; lignes rejetées en JSON, qui sait écrire
  une liste comme preferred_categories, contrairement au CSV.
### Programmes de test
- TestFMC, TestAdji et TestBland sont rangés dans src/test/scala : ils restent dans le dépôt
  et montrent comment chaque module a été vérifié, sans entrer dans le JAR.
### Règle Git du groupe
- git pull --no-edit avant chaque git push, jamais de push forcé : un push forcé efface de
  GitHub les commits des autres membres.

### UDF extractTimeFeatures (question 3.1)
- Lecture stricte des dates (motif uuuuMMddHHmmss, ResolverStyle.STRICT) : une date impossible
  comme le 31 février donne null au lieu d'être corrigée en silence en 29 février.
- Jours et mois en anglais (Locale.ENGLISH) : même résultat sur tous les PC, quelle que soit
  leur langue, et cohérent avec les étiquettes Morning/Afternoon/Evening/Night du sujet.
- L'heure est un entier de 0 à 23, pour être comparée et triée sans conversion.
- Robustesse : Try attrape toute erreur de lecture. Un timestamp null, vide ou mal formé donne
  six colonnes nulles sans faire échouer le job (700 cas dans le fichier brut).
- La structure rendue par l'UDF est dépliée en six colonnes simples, car le CSV ne sait pas
  écrire une structure.
- La plage 0h-6h n'appartient à aucune période du sujet : elle est classée Night, puisque
    la nuit continue après minuit.
- is_working_hours = 1 si 9 <= heure < 17 : une journée de travail se termine à 17h00.

### Journal de Spark 
- Fichier log4j2.properties dans src/main/resources, niveau WARN : les ~40 lignes INFO
  du démarrage disparaissent. setLogLevel("WARN") seul ne suffit pas, car il s'exécute
  après la création de la session, quand ces lignes sont déjà écrites.
- Le message ERROR de ShutdownHookManager (dossier temporaire non effacé sous Windows)
  est éteint : défaut connu, sans effet sur les résultats. Les autres erreurs restent visibles.

### Jointures de la question 3.2 
- transactions : table de départ, une ligne par achat valide (136 157).
- users : left. L'achat a eu lieu même si le profil est faux ou absent ; ces lignes
  reçoivent age_group = "Inconnu" (4 175 achats).
- products : left. Aucun KPI obligatoire n'utilise les colonnes produit, perdre ces ventes
  n'apporterait rien.
- merchants : inner. Le rapport 4.1 a besoin de la région, de la catégorie et d'un taux de
  commission valide ; 3 133 achats (2,3 %) sont écartés. Résultat : 133 024 lignes.
- Les trois petites tables sont diffusées (broadcast) si enable-broadcast = true.
- Jeune : moins de 25 ans ; Adulte : 25 à 44 ; Âge Moyen : 45 à 64 ; Senior : 65 et plus.
- Le sujet laisse 25 ans sans tranche (Jeune s'arrête avant 25, Adulte commence à 26).
  Les 210 utilisateurs de 25 ans sont classés Adulte, ce qui garde intacte la règle
  « moins de 25 ans ». Âge absent (jointure left) : Inconnu.

### Fenêtres des questions 3.2 et 3.3 
- Calculées après les jointures : le rang va de 1 à n sans trou et user_tx_count
  égale le nombre de lignes du client dans le tableau final.
- Rang : row_number, ordre timestamp puis transaction_id (un numéro unique par achat).
- Temps : secondes depuis 1970 calculées par TimeFeatures, sans fuseau horaire, pour un
  résultat identique sur tous les PC (pas d'heure d'été, pas d'échec ANSI).
- rolling_7d_amount : achats du client dans les 7 × 24 h qui précèdent, l'achat compris.
- is_active_user : 1 si le client a acheté au moins 5 dates distinctes sur le jour de
  l'achat et les 6 jours d'avant (1 070 achats, 508 clients).
- days_since_prev_tx : écart en jours décimaux arrondi à 2 décimales, vide pour le
  premier achat.

### Transactions suspectes (bonus 3.4)
- Panier moyen = moyenne de tous les achats du client dans le tableau final, achat compris.
- Condition montant : écart non arrondi supérieur à 300 %, soit plus de 4 fois la moyenne.
- Délai recalculé en secondes (lag sur l'horloge de TimeFeatures) : days_since_prev_tx,
  arrondi en jours, ne permet pas de voir 5 minutes.
- Chaque condition vaut when(condition, 1).otherwise(0) : une valeur vide (premier achat,
  moyen de paiement absent) compte 0. Suspect si au moins deux conditions sur quatre.
- Résultat : 2 943 transactions suspectes, chez 2 184 clients.

### KPI par marchand (question 4.1)
- Les comptes sont faits par groupBy, puis les classements par fenêtre sur ce tableau d'une
  ligne par marchand : pour classer un marchand, il faut d'abord connaître son CA total.
- Classements avec rank : deux marchands ex aequo ont la même place, et le résultat est le
  même à chaque lancement (row_number les départagerait au hasard).
- « Dans sa catégorie » désigne merchant_category, la catégorie du marchand, et non celle
  de chaque produit vendu.
- CA par tranche d'âge par somme conditionnelle (when / otherwise 0) plutôt que pivot :
  noms de colonnes sans accent ni espace, et 0 au lieu d'une case vide.
- nb_clients_uniques compte les user_id distincts, orphelins compris ; commission totale
  = somme de amount × commission_rate ; arrondis à 2 décimales en fin de calcul.
- 9 marchands valides n'ont pas de région (la validation 2.2 ne contrôle que la commission) :
  ils restent dans le rapport et sont classés entre eux, dans une région vide.
- Bonus : nb_transactions_suspectes et taux_suspects ne sont ajoutés que si le tableau reçu
  contient is_suspicious. Taux global : 2,2 %.

### Analyse de cohortes (question 4.2)
- Le mois vient des 6 premiers caractères du timestamp (yyyyMM) : les mois en texte anglais
  de l'UDF se trieraient par ordre alphabétique.
- period_index est calculé sur un numéro de mois (année × 12 + mois), pour passer
  correctement de décembre 2024 à janvier 2025.
- Utilisateurs comptés avec count_distinct ; la taille initiale de chaque cohorte est
  recollée à la matrice par broadcast (24 lignes seulement).
- Meilleure rétention à 3 mois : les cohortes d'octobre à décembre 2025 sont exclues, car
  leur troisième mois n'est pas encore observé. Résultat : 202506, 36,2 %.
- Bonus : chiffre d'affaires et revenu moyen par utilisateur pour chaque cohorte et période.

### Optimisations (questions 5.1 et 5.2)
- cache sur les quatre fichiers lus et sur les rapports 4.1 et 4.2, utilisés plusieurs fois.
- persist(MEMORY_AND_DISK_SER) sur le tableau enrichi (133 024 lignes, 35 colonnes) : s'il
  ne tient pas en mémoire, il déborde sur le disque au lieu d'être recalculé.
- unpersist dès qu'un tableau ne sert plus, pour libérer la mémoire.
- broadcast des petites tables dans les jointures, et spark.sql.shuffle.partitions = 8 au
  lieu de 200 : 200 morceaux seraient presque vides pour 138 000 lignes.
- Cache et broadcast se coupent avec enable-cache et enable-broadcast dans
  application.conf ; les résultats restent identiques, seule la durée change.

### Application principale (question 6.1)
- SparkSessionBuilder ne prend le master d'application.conf que si spark-submit n'en a pas
  donné : un master écrit dans le code empêcherait de lancer le JAR sur un cluster.
- Le pipeline tourne dans un try/catch/finally : spark.stop() dans le finally, code de
  sortie 1 en cas d'échec.
- Chaque résultat est écrit en CSV (un seul fichier, coalesce(1)) et en Parquet, dans
  output/csv/<nom> et output/parquet/<nom>, en mode overwrite. Le tableau enrichi n'est
  écrit qu'en Parquet.
- spark.sql.debug.maxToStringFields = 100 : le journal ne coupe plus le plan d'exécution.
- Imports des fonctions Spark en liste explicite (functions.{col, when, ...}), règle
  adoptée ensuite par tout le groupe.
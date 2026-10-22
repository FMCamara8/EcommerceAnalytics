# Journal de contribution

## Répartition

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

## Charge de travail et difficultés

## Décisions techniques du groupe
### Chemins des données
Les chemins sont relatifs à la racine du projet (src/main/resources/data/...), car Spark
ne peut pas lire un fichier placé à l'intérieur du JAR. Le programme se lance donc depuis
la racine du projet. Chaque chemin peut être remplacé au lancement avec -Dcle=valeur,
sans recompiler.

## Relectures

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
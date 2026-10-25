package com.ecommerce.analytics

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import com.ecommerce.utils.ConfigLoader

object SparkOptimizations {

  // 5.1 : garder en mémoire un tableau réutilisé plusieurs fois
  def cacheIfEnabled[T](ds: Dataset[T]): Dataset[T] =
    if (ConfigLoader.enableCache) ds.cache() else ds

  // 5.1 : pour un gros tableau, mémoire compactée d'abord, disque pour le reste
  def persistLarge[T](ds: Dataset[T]): Dataset[T] =
    if (ConfigLoader.enableCache) ds.persist(StorageLevel.MEMORY_AND_DISK_SER) else ds

  // 5.1 : libérer la mémoire quand on n'en a plus besoin
  def release(datasets: Dataset[_]*): Unit =
    datasets.foreach(ds => ds.unpersist())

  // 5.2 : envoyer une copie d'une petite table à chaque exécuteur
  def broadcastIfEnabled[T](ds: Dataset[T]): Dataset[T] =
    if (ConfigLoader.enableBroadcast) broadcast(ds) else ds
}
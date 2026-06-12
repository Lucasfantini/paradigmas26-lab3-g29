import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Accumulators para estadísticas
    val feedsSuccessAcc = sc.longAccumulator("feedsSuccess")
    val feedsFailedAcc = sc.longAccumulator("feedsFailed")
    val postsDownloadedAcc = sc.longAccumulator("postsDownloaded")
    val postsFilteredAcc = sc.longAccumulator("postsFiltered")

    // Parallelize subscriptions
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds, parse posts and filter valid posts in parallel
    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      try {
        val feedOpt = FileIO.downloadFeed(subscription.url)

        feedOpt match {
          case Some(json) =>
            feedsSuccessAcc.add(1)

            val posts = JsonParser.parsePosts(json, subscription)
            postsDownloadedAcc.add(posts.length)

            val filteredPosts = Analyzer.filterEmptyPosts(posts)
            postsFilteredAcc.add(posts.length - filteredPosts.length)

            filteredPosts

          case None =>
            feedsFailedAcc.add(1)
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            List.empty[Post]
        }
      } catch {
        case _: Exception =>
          feedsFailedAcc.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
      }
    }.cache() 
    /* 
     * Al agregar cache aca, en el primer collec() Spark descarga los feeds
     * y guarda el resultado en memoria. 
     * Luego, al volver a usar postsRDD para extraer entidades, 
     * no vuelve a descargar los feeds sino que lee los posts
     * desde memoria
     */

    // Medicion de descarga y filtrado de posts

    val startPosts = System.currentTimeMillis()

    val filteredPosts = postsRDD.collect().toList

    val endPosts = System.currentTimeMillis()

    println(s"Tiempo descarga y filtrado: ${(endPosts - startPosts) / 1000.0} segundos")

    val feedsSuccess = feedsSuccessAcc.value.toInt
    val feedsFailed = feedsFailedAcc.value.toInt
    val postsSuccess = postsDownloadedAcc.value.toInt
    val postsFiltered = postsFilteredAcc.value.toInt

    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> feedsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      postsRDD.unpersist()
      spark.stop()
      return
    }

    // Desde acá sigue igual que antes, todavía secuencial por ahora
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Para cada post, extraer sus entidades nombradas y devolver un iterador de NamedEntity.
    val entitiesRDD = postsRDD.flatMap { post =>
      val text = post.title + " " + post.selftext
      Analyzer.detectEntities(text, dictionary)
    }

    // convertir cada NamedEntity en un par (tipo, nombre) como clave y 1 como valor
    val entityPairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    //sumar los valores de cada clave para obtener el conteo total por entidad
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)

    // Medicion procesamiento entidades + reduceByKey
    val startEntities = System.currentTimeMillis()

    // orden descendente por conteo, luego alfabético por tipo y nombre
    val results = entityCountsRDD
      .collect()
      .sortBy { case ((entityType, entityName), count) =>
        (-count, entityType, entityName)
      }

    val endEntities = System.currentTimeMillis()

    println(s"Tiempo procesamiento entidades: ${(endEntities - startEntities) / 1000.0} segundos")
    
    // mostrar resultados
    results.foreach {
      case ((entityType, entityName), count) =>
        println(s"[$entityType] $entityName: $count apariciones")
    }
    
    /*
    * Lo agrego para liberar la memoria ocupada por los posts descargados
    */
    postsRDD.unpersist()

    spark.stop()
  }
}

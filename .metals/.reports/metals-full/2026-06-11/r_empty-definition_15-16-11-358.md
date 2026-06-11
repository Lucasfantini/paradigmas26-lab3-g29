error id: file://<WORKSPACE>/src/main/scala/Main.scala:
file://<WORKSPACE>/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -combinedText.
	 -combinedText#
	 -combinedText().
	 -scala/Predef.combinedText.
	 -scala/Predef.combinedText#
	 -scala/Predef.combinedText().
offset: 5527
uri: file://<WORKSPACE>/src/main/scala/Main.scala
text:
```scala
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

            val posts = JsonParser.parsePosts(json, subscription.name)
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
    }

    // Acción: Spark recién ejecuta el flatMap acá
    val filteredPosts = postsRDD.collect().toList

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
      spark.stop()
      return
    }

    /*
    * Ejercicio 3:
    * Hasta el momento que lo que hicimos fue usar spark hasta 
    * cierto punto donde collect() lleva todo al driver
    *
    * Ahora lo que queremos hacer es seguir usando spark despues
    * de los posts
    */
    object Dictionary {

  /**
   * Load entities from a dictionary file and create instances of the specified type.
   * @param filePath path to dictionary file (e.g., "data/people.txt")
   * @param entityType type of entity to create ("Person", "University", etc.)
   * @return Option containing list of entities, None if file missing
   */
  def loadFromFile(filePath: String, entityType: String): Option[List[NamedEntity]] = {
    FileIO.readDictionaryFile(filePath).map { lines =>
      lines.map { name =>
        entityType match {
          case "Person"              => new Person(name)
          case "Organization"        => new Organization(name)
          case "University"          => new University(name)
          case "Place"               => new Place(name)
          case "Technology"          => new Technology(name)
          case "ProgrammingLanguage" => new ProgrammingLanguage(name)
          case _                     => new Person(name) // fallback
        }
      }
    }
  }

  /**
   * Load all dictionary files and combine into a single list.
   * Prints warnings for missing dictionary files and continues with others.
   * @param entitiesDir path to directory containing entity files
   * @return combined list of all entities from all successfully loaded dictionaries
   */
  def loadAll(entitiesDir: String): List[NamedEntity] = {
    // Check if entities directory exists
    val dataDir = new java.io.File(entitiesDir)

    val peopleOpt = loadFromFile(s"$entitiesDir/people.txt", "Person")

    val universitiesOpt = loadFromFile(s"$entitiesDir/universities.txt", "University")

    val languagesOpt = loadFromFile(s"$entitiesDir/languages.txt", "ProgrammingLanguage")

    val organizationsOpt = loadFromFile(s"$entitiesDir/organizations.txt", "Organization")

    val placesOpt = loadFromFile(s"$entitiesDir/places.txt", "Place")

    peopleOpt.getOrElse(List()) :::
      universitiesOpt.getOrElse(List()) :::
      languagesOpt.getOrElse(List()) :::
      organizationsOpt.getOrElse(List()) :::
      placesOpt.getOrElse(List())
  }
}


    val entitiesRDD = postsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(co@@mbinedText, dictionary)
    }

    val entityCountsRDD = entitiesRDD
      .map(entity => ((entity.entityType, entity.text), 1))
      .reduceByKey(_ + _)

    val entityCounts = entityCountsRDD.collect().toMap

    val allEntities = entitiesRDD.collect().toList
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 
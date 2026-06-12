import java.io.File

object Dictionary {

  private val entityFiles = List(
    ("people.txt", "Person"),
    ("universities.txt", "University"),
    ("languages.txt", "ProgrammingLanguage"),
    ("organizations.txt", "Organization"),
    ("places.txt", "Place")
  )

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
          case _                     => new Person(name)
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
    val dataDir = new File(entitiesDir)

    if (!dataDir.exists() || !dataDir.isDirectory) {
      println(s"Error: entities directory '$entitiesDir' not found")
      List.empty
    } else {
      entityFiles.flatMap { case (fileName, entityType) =>
        val filePath = s"$entitiesDir/$fileName"

        loadFromFile(filePath, entityType) match {
          case Some(entities) => entities
          case None =>
            println(s"Warning: Could not load $filePath")
            List.empty[NamedEntity]
        }
      }
    }
  }
}

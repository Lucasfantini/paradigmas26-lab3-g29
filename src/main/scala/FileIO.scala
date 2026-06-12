import java.io.FileNotFoundException
import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  private def closeQuietly(source: Source): Unit = {
    if (source != null) {
      try source.close()
      catch { case _: Exception => () }
    }
  }

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found or invalid JSON
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats
    var source: Source = null

    try {
      source = Source.fromFile(filePath)
      val content = source.mkString
      val json = parse(content)

      json.extract[List[JValue]].map { sub =>
        val name = (sub \ "name").extractOpt[String].map(_.trim).filter(_.nonEmpty)
        val url = (sub \ "url").extractOpt[String].map(_.trim).filter(_.nonEmpty)

        (name, url) match {
          case (Some(subscriptionName), Some(subscriptionUrl)) =>
            Some(Subscription(subscriptionName, subscriptionUrl))
          case _ =>
            println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
            None
        }
      }
    } catch {
      case _: FileNotFoundException =>
        println(s"Error: Could not load $filePath - file not found")
        List.empty
      case _: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        List.empty
    } finally {
      closeQuietly(source)
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    var source: Source = null

    try {
      source = Source.fromURL(url)
      Some(source.mkString)
    } catch {
      case _: Exception => None
    } finally {
      closeQuietly(source)
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file is missing or unreadable
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    var source: Source = null

    try {
      source = Source.fromFile(filePath)
      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList
      Some(lines)
    } catch {
      case _: Exception => None
    } finally {
      closeQuietly(source)
    }
  }
}

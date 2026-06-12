import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent JSON string from Reddit API
   * @param subscription subscription being parsed (for logging)
   * @return list of posts, empty list if parsing fails
   */
  def parsePosts(jsonContent: String, subscription: Subscription): List[Post] = {
    implicit val formats: Formats = DefaultFormats

    def warnParseFailure(): Unit = {
      println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
    }

    try {
      val json = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]

      children.flatMap { child =>
        try {
          val data = child \ "data"
          val title = (data \ "title").extract[String]
          val selftext = (data \ "selftext").extract[String]
          List(Post(title, selftext))
        } catch {
          case _: Exception =>
            warnParseFailure()
            List.empty[Post]
        }
      }
    } catch {
      case _: Exception =>
        warnParseFailure()
        List.empty[Post]
    }
  }

  def parsePosts(jsonContent: String, subscriptionName: String): List[Post] = {
    parsePosts(jsonContent, Subscription(subscriptionName, "unknown"))
  }
}

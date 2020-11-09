/**
 * Movie Score Parser
 * From JSON movies data, create Neo4j database with nodes
 * and relationships between Movies, Peoples and Genres
 * Jeremy Favre & Steven Liatti
 */

package ch.hepia

import ch.hepia.Domain.{Actor, Credits, Genre, Movie}
import spray.json.DefaultJsonProtocol

/**
 * Formats definitions to parse JSON with spray
 */
object JsonFormats  {
  import DefaultJsonProtocol._

  implicit val genreFormat = jsonFormat2(Genre)
  implicit val actorFormat = jsonFormat9(Actor)
  implicit val creditFormat = jsonFormat2(Credits)
  implicit val movieFormat = jsonFormat12(Movie)
}

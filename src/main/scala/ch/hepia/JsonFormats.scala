/**
 * Movie Score Parser
 * From JSON movies data, create Neo4j database with nodes
 * and relationships between Movies, Peoples and Genres
 * Jeremy Favre & Steven Liatti
 */

package ch.hepia

import ch.hepia.Domain.Actor
import ch.hepia.Domain.Credits
import ch.hepia.Domain.Genre
import ch.hepia.Domain.Movie
import ch.hepia.Domain.PlayInMovie
import ch.hepia.Domain.ProductionCountries
import spray.json.DefaultJsonProtocol

/**
 * Formats definitions to parse JSON with spray
 */
object JsonFormats  {
  import DefaultJsonProtocol._

  implicit val genreFormat = jsonFormat2(Genre)
  implicit val playInMovieFormat = jsonFormat3(PlayInMovie)
  implicit val creditFormat = jsonFormat1(Credits)
  implicit val actorFormat = jsonFormat9(Actor)
  implicit val productionCountriesFormat = jsonFormat2(ProductionCountries)
  implicit val movieFormat = jsonFormat13(Movie)
}

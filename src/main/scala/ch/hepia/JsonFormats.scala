/**
 * Movie Score Parser
 * From JSON movies data, create Neo4j database with nodes
 * and relationships between Movies, Peoples and Genres
 * Jeremy Favre & Steven Liatti
 */

package ch.hepia

import ch.hepia.Domain.{Actor, Credits, Genre, Movie, MovieId, MovieMaker, Recommendations, Similar}
import spray.json.DefaultJsonProtocol

/**
 * Formats definitions to parse JSON with spray
 */
object JsonFormats  {
  import DefaultJsonProtocol._

  implicit val genreFormat = jsonFormat2(Genre)
  implicit val actorFormat = jsonFormat5(Actor)
  implicit val crewFormat = jsonFormat4(MovieMaker)
  implicit val creditFormat = jsonFormat2(Credits)
  implicit val similarMovieIdFormat = jsonFormat1(MovieId)
  implicit val similarFormat = jsonFormat1(Similar)
  implicit val recommendationsFormat = jsonFormat1(Recommendations)
  implicit val movieFormat = jsonFormat8(Movie)
}

/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

/**
  * App domain, trait and case classes that represent
  * the different entities
  */
object Domain {
  case class PlayInMovie(id: Long, character: Option[String], order: Int)
  case class Credits(cast: List[PlayInMovie])

  case class Actor(
      id: Long,
      name: String,
      biography: Option[String],
      birthday: Option[String],
      deathday: Option[String],
      gender: Int,
      place_of_birth: Option[String],
      profile_path: Option[String],
      movie_credits: Credits
  ) {
    def intToGender(): String =
      gender match {
        case 1 => "Female"
        case 2 => "Male"
        case _ => "Undefined"
      }
  }

  case class ProductionCountries(iso_3166_1: String, name: String)
  case class Movie(
      id: Long,
      title: String,
      overview: String,
      budget: Long,
      revenue: Long,
      genres: List[Genre],
      credits: Credits,
      backdrop_path: Option[String],
      poster_path: Option[String],
      production_countries: Option[List[ProductionCountries]],
      release_date: Option[String],
      runtime: Option[Int],
      tagline: Option[String]
  )

  case class Genre(id: Long, name: String)
}

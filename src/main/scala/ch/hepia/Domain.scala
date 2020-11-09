/**
  * Movie Score Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Peoples and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

/**
  * App domain, trait and case classes that represent
  * the different entities
  */
object Domain {

  trait People {
    def id: Long
    def name: String
    def gender: Int

    def intToGender(): String =
      gender match {
        case 1 => "Female"
        case 2 => "Male"
        case _ => "Undefined"
      }
  }

  case class Credits(cast: List[PlayInMovie])
  case class PlayInMovie(id: Long, character: String, order: Int)

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
  ) extends People

  case class ProductionCountries(iso_3166_1: String, name: String)
  case class Movie(
      id: Long,
      title: String,
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

  case class SimplePeople(id: Long, name: String, gender: String)
  case class MovieForPeople(movieId: Long, people: People, score: Double)
  case class GenreForPeople(genreId: Long, people: People)
}

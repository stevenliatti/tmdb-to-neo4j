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

    def intToGender(): String = gender match {
      case 1 => "Female"
      case 2 => "Male"
      case _ => "Undefined"
    }
  }

  case class Genre(id: Long, name: String)
  case class Actor(id: Long, name: String, gender: Int, order: Int, character: String) extends People
  case class MovieMaker(id: Long, name: String, gender: Int, job: String) extends People
  case class Credits(cast: List[Actor], crew: List[MovieMaker])
  case class MovieId(id: Long)
  case class Similar(results: List[MovieId])
  case class Recommendations(results: List[MovieId])
  case class Movie(id: Long,
                   title: String,
                   budget: Long,
                   revenue: Long,
                   genres: List[Genre],
                   credits: Credits,
                   similar: Option[Similar],
                   recommendations: Option[Recommendations])

  case class SimplePeople(id: Long, name: String, gender: String)
  case class MovieForPeople(movieId: Long, people: People, score: Double)
  case class GenreForPeople(genreId: Long, people: People)
}

/**
 * Movie Score Parser
 * From JSON movies data, create Neo4j database with nodes
 * and relationships between Movies, Peoples and Genres
 * Jeremy Favre & Steven Liatti
 */

package ch.hepia

import ch.hepia.Domain._
import neotypes.Driver
import neotypes.implicits._
import spray.json.JsonParser

import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

/**
 * Main service class who read JSON movies
 * and give methods to add nodes and relationships
 * in Neo4j with Cypher requests
 * @param driver neo4j scala driver
 */
class MovieService(driver: Driver[Future]) {

  def computeMovieScore(movie: Movie): Double = {
    movie.revenue.toDouble / movie.budget.toDouble
  }

  def createConstraints(): Future[Unit] = driver.readSession { session =>
    c"""
      CREATE CONSTRAINT ON (m:Movie) ASSERT m.id IS UNIQUE;
      CREATE CONSTRAINT ON (g:Genre) ASSERT g.id IS UNIQUE;
      CREATE CONSTRAINT ON (p:People) ASSERT p.id IS UNIQUE;
      CREATE INDEX ON :Movie(title);
    """.query[Unit].execute(session)
  }

  def addMovie(movie: Movie): Future[Unit] = driver.readSession { session =>
    val score: Double = computeMovieScore(movie)

    c"""CREATE (movie: Movie {
        id: ${movie.id},
        title: ${movie.title},
        budget:${movie.budget},
        revenue:${movie.revenue},
        score: $score
     })""".query[Unit].execute(session)
  }

  def addGenres(genre: Genre): Future[Unit] = driver.readSession { session =>
    c"""MERGE (genre: Genre {
      id: ${genre.id},
      name: ${genre.name}})""".query[Unit].execute(session)
  }

  def addPeople(people: SimplePeople, score: Double, labels: mutable.Set[String]): Future[Unit] = driver.readSession { session =>
      if (labels("Actor") && labels("MovieMaker")) {
        c"""CREATE (p:People {
          id: ${people.id},
          name: ${people.name},
          gender: ${people.gender},
          score: $score
        })
        SET p: Actor
        SET p: MovieMaker""".query[Unit].execute(session)
      }
      else if (labels("Actor")) {
        c"""CREATE (p:People {
          id: ${people.id},
          name: ${people.name},
          gender: ${people.gender},
          score: $score
        })
        SET p: Actor""".query[Unit].execute(session)
      }
      else {
        c"""CREATE (p:People {
          id: ${people.id},
          name: ${people.name},
          gender: ${people.gender},
          score: $score
        })
        SET p: MovieMaker""".query[Unit].execute(session)
      }
  }

  def addInMoviesRelation(people: People, movieId: Long) : Future[Unit] = driver.readSession { session =>
    people match {
      case a: Actor =>
        c"""MATCH (m: Movie {id: $movieId})
        MATCH (p: Actor {id: ${people.id}})
        MERGE (p)-[r:PLAY_IN {character: ${a.character}}]->(m)
        """.query[Unit].execute(session)
      case mm: MovieMaker =>
        c"""MATCH (m: Movie {id: $movieId})
        MATCH (p: MovieMaker {id: ${people.id}})
        MERGE (p)-[r:WORK_IN {job: ${mm.job}}]->(m)
        """.query[Unit].execute(session)
    }
  }

  def addKnownForRelation(people: People, genreId: Long, count: Int) : Future[Unit] = driver.readSession { session =>
    people match {
      case _: Actor =>
        c"""MATCH (p: People {id: ${people.id}})
        MATCH (g: Genre {id: $genreId})
        MERGE (p)-[r:KNOWN_FOR_ACTING {count: $count}]->(g)
     """.query[Unit].execute(session)
      case _: MovieMaker =>
        c"""MATCH (p: People {id: ${people.id}})
        MATCH (g: Genre {id: $genreId})
        MERGE (p)-[r:KNOWN_FOR_WORKING {count: $count}]->(g)
     """.query[Unit].execute(session)
    }
  }

  def addKnowsRelation(pId1: Long, pId2: Long, count: Int) : Future[Unit] = driver.readSession { session =>
    c"""MATCH (p1: People {id: $pId1})
        MATCH (p2: People {id: $pId2})
        MERGE (p1)-[r:KNOWS {count: $count}]-(p2)
     """.query[Unit].execute(session)
  }

  def addSimilarRelation(movie: Movie, movieId: MovieId) : Future[Unit] = driver.readSession { session =>
    c"""MATCH (m1: Movie {id: ${movie.id}})
        MATCH (m2: Movie {id: ${movieId.id}})
        MERGE (m1)-[r:SIMILAR]-(m2)
     """.query[Unit].execute(session)
  }

  def addRecommendationsRelation(movie: Movie, movieId: MovieId) : Future[Unit] = driver.readSession { session =>
    c"""MATCH (m1: Movie {id: ${movie.id}})
        MATCH (m2: Movie {id: ${movieId.id}})
        MERGE (m1)-[r:RECOMMENDATIONS]-(m2)
     """.query[Unit].execute(session)
  }

  def addMoviesGenres(movie: Movie, genre: Genre) : Future[Unit] = driver.readSession { session =>
    c"""MATCH (m: Movie {id: ${movie.id}})
        MATCH (g: Genre {id: ${genre.id}})
        MERGE (m)-[r:BELONGS_TO]->(g)
     """.query[Unit].execute(session)
  }

  def readMoviesFromFile(path: String): Iterator[Movie] = {
    import JsonFormats._

    Source.fromFile(path).getLines
      .map(line => JsonParser(line).convertTo[Movie])
  }

}

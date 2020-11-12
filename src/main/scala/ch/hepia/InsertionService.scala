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
class InsertionService(driver: Driver[Future]) {

  def createConstraints(): Future[Unit] =
    driver.readSession { session =>
      c"""
      CREATE CONSTRAINT ON (m:Movie) ASSERT m.id IS UNIQUE;
      CREATE CONSTRAINT ON (g:Genre) ASSERT g.id IS UNIQUE;
      CREATE CONSTRAINT ON (a:Actor) ASSERT a.id IS UNIQUE;
      CREATE INDEX ON :Movie(title);
      CREATE INDEX ON :Actor(name);
    """.query[Unit].execute(session)
    }

  def addMovie(movie: Movie): Future[Unit] =
    driver.readSession { session =>
      c"""CREATE (movie: Movie {
        id: ${movie.id},
        title: ${movie.title},
        budget:${movie.budget},
        revenue:${movie.revenue},
     })""".query[Unit].execute(session)
    }

  def addGenres(genre: Genre): Future[Unit] =
    driver.readSession { session =>
      c"""MERGE (genre: Genre {
      id: ${genre.id},
      name: ${genre.name}})""".query[Unit].execute(session)
    }

  def addActor(actor: Actor): Future[Unit] =
    driver.readSession { session =>
      c"""CREATE (actor: Actor {
        id: ${actor.id},
        name: ${actor.name},
        biography:${actor.biography},
        birthday:${actor.birthday},
        deathday:${actor.deathday},
        gender:${actor.gender},
        place_of_birth:${actor.place_of_birth},
        profile_path:${actor.profile_path},
        movie_credits:${actor.movie_credits},
     })""".query[Unit].execute(session)

    }

  // def addInMoviesRelation(people: People, movieId: Long) : Future[Unit] = driver.readSession { session =>
  //   people match {
  //     case a: Actor =>
  //       c"""MATCH (m: Movie {id: $movieId})
  //       MATCH (p: Actor {id: ${people.id}})
  //       MERGE (p)-[r:PLAY_IN {character: ${a.character}}]->(m)
  //       """.query[Unit].execute(session)
  //     case mm: MovieMaker =>
  //       c"""MATCH (m: Movie {id: $movieId})
  //       MATCH (p: MovieMaker {id: ${people.id}})
  //       MERGE (p)-[r:WORK_IN {job: ${mm.job}}]->(m)
  //       """.query[Unit].execute(session)
  //   }
  // }

  // def addKnownForRelation(people: People, genreId: Long, count: Int) : Future[Unit] = driver.readSession { session =>
  //   people match {
  //     case _: Actor =>
  //       c"""MATCH (p: People {id: ${people.id}})
  //       MATCH (g: Genre {id: $genreId})
  //       MERGE (p)-[r:KNOWN_FOR_ACTING {count: $count}]->(g)
  //    """.query[Unit].execute(session)
  //     case _: MovieMaker =>
  //       c"""MATCH (p: People {id: ${people.id}})
  //       MATCH (g: Genre {id: $genreId})
  //       MERGE (p)-[r:KNOWN_FOR_WORKING {count: $count}]->(g)
  //    """.query[Unit].execute(session)
  //   }
  // }

  def addKnowsRelation(
      aId1: Long,
      aId2: Long,
      movieId: Long
  ): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (a1: People {id: $aId1})
        MATCH (a2: People {id: $aId2})
        MERGE (a1)-[r:KNOWS {movieId: $movieId}]-(a2)
     """.query[Unit].execute(session)
    // En construction
    // 2 possibilités soit on passe par noeud movie intermediaire soit on store liste dans relation
    }

  def addMoviesGenres(movie: Movie, genre: Genre): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (m: Movie {id: ${movie.id}})
        MATCH (g: Genre {id: ${genre.id}})
        MERGE (m)-[r:BELONGS_TO]->(g)
     """.query[Unit].execute(session)
    }

  def readMoviesFromFile(path: String): Iterator[Movie] = {
    import JsonFormats._

    Source
      .fromFile(path)
      .getLines
      .map(line => JsonParser(line).convertTo[Movie])
  }
  
  def readActorsFromFile(path: String): Iterator[Actor] = {
    import JsonFormats._

    Source
      .fromFile(path)
      .getLines
      .map(line => JsonParser(line).convertTo[Actor])
  }

}

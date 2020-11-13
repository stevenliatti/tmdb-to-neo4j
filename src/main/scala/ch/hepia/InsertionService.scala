/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
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
      CREATE INDEX ON :Genre(name);
    """.query[Unit].execute(session)
    }

  def addMovie(movie: Movie): Future[Unit] =
    // TODO check production_countries
    driver.readSession { session =>
      c"""CREATE (movie: Movie {
        id: ${movie.id},
        title: ${movie.title},
        overview: ${movie.overview},
        budget: ${movie.budget},
        revenue: ${movie.revenue},
        backdrop_path: ${movie.backdrop_path},
        poster_path: ${movie.poster_path},
        production_countries: ${movie.production_countries},
        release_date: ${movie.release_date},
        runtime: ${movie.runtime},
        tagline: ${movie.tagline},
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
        biography: ${actor.biography},
        birthday: ${actor.birthday},
        deathday: ${actor.deathday},
        gender: ${actor.intToGender()},
        place_of_birth: ${actor.place_of_birth},
        profile_path: ${actor.profile_path},
     })""".query[Unit].execute(session)

    }

  def addPlayInRelation(actor: PlayInMovie, movieId: Long): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (m: Movie {id: $movieId})
        MATCH (a: Actor {id: ${actor.id}})
        MERGE (a)-[r:PLAY_IN {character: ${actor.character}}]->(m)
        """.query[Unit].execute(session)
    }

  def addKnownForRelation(
      actorId: Long,
      genreId: Long,
      count: Int
  ): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (a: Actor {id: ${actorId}})
        MATCH (g: Genre {id: $genreId})
        MERGE (a)-[r:KNOWN_FOR {count: $count}]->(g)
     """.query[Unit].execute(session)
    }

  def addKnowsRelation(
      aId1: Long,
      aId2: Long,
      movieId: Long
  ): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (a1: Actor {id: $aId1})
        MATCH (a2: Actor {id: $aId2})
        MERGE (a1)-[r:KNOWS {movieId: $movieId}]-(a2)
     """.query[Unit].execute(session)
    }

  def addMoviesGenres(movieId: Long, genreId: Long): Future[Unit] =
    driver.readSession { session =>
      c"""MATCH (m: Movie {id: ${movieId}})
        MATCH (g: Genre {id: ${genreId}})
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

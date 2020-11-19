/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

import neotypes.GraphDatabase
import neotypes.implicits.all._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import neotypes.Driver
import ch.hepia.Domain._
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
    driver.writeSession { session =>
      c"""
        CREATE CONSTRAINT ON (m:Movie) ASSERT m.tmdbId IS UNIQUE;
        CREATE CONSTRAINT ON (g:Genre) ASSERT g.tmdbId IS UNIQUE;
        CREATE CONSTRAINT ON (a:Actor) ASSERT a.tmdbId IS UNIQUE;
        CREATE CONSTRAINT ON (c:Country) ASSERT c.iso_3166_1 IS UNIQUE;
        CREATE INDEX ON :Movie(title);
        CREATE INDEX ON :Actor(name);
        CREATE INDEX ON :Genre(name);
    """.query[Unit].execute(session)
    }

  def addMovie(movie: Movie): Future[Unit] =
    driver.writeSession { session =>
      c"""
        CREATE (movie: Movie {
          tmdbId: ${movie.id},
          title: ${movie.title},
          overview: ${movie.overview},
          budget: ${movie.budget},
          revenue: ${movie.revenue},
          backdrop_path: ${movie.backdrop_path.getOrElse("")},
          poster_path: ${movie.poster_path.getOrElse("")},
          release_date: ${movie.release_date.getOrElse("")},
          runtime: ${movie.runtime.getOrElse(-1)},
          tagline: ${movie.tagline.getOrElse("")}
        })
      """.query[Unit].execute(session)
    }

  def addCountry(country: ProductionCountries): Future[Unit] =
    driver.writeSession { session =>
      c"""
        CREATE (country: Country {
          iso_3166_1: ${country.iso_3166_1},
          name: ${country.name}
        })
      """.query[Unit].execute(session)
    }

  def addGenre(genre: Genre): Future[Unit] =
    driver.writeSession { session =>
      c"""
        CREATE (genre: Genre {
          tmdbId: ${genre.id},
          name: ${genre.name}
        })
      """.query[Unit].execute(session)
    }

  def addActor(actor: Actor): Future[Unit] =
    driver.writeSession { session =>
      c"""
        CREATE (actor: Actor {
          tmdbId: ${actor.id},
          name: ${actor.name},
          biography: ${actor.biography.getOrElse("")},
          birthday: ${actor.birthday.getOrElse("")},
          deathday: ${actor.deathday.getOrElse("")},
          gender: ${actor.intToGender()},
          place_of_birth: ${actor.place_of_birth.getOrElse("")},
          profile_path: ${actor.profile_path.getOrElse("")}
        })
     """.query[Unit].execute(session)
    }

  def addPlayInRelation(actor: PlayInMovie, movieId: Long): Future[Unit] =
    driver.writeSession { session =>
      c"""
        MATCH (m: Movie {tmdbId: $movieId})
        MATCH (a: Actor {tmdbId: ${actor.id}})
        MERGE (a)-[r:PLAY_IN {character: ${actor.character}, order: ${actor.order}}]->(m)
      """.query[Unit].execute(session)
    }

  def addKnownForRelation(
      actorId: Long,
      genreId: Long,
      count: Int
  ): Future[Unit] =
    driver.writeSession { session =>
      c"""
        MATCH (a: Actor {tmdbId: ${actorId}})
        MATCH (g: Genre {tmdbId: $genreId})
        MERGE (a)-[r:KNOWN_FOR {count: $count}]->(g)
      """.query[Unit].execute(session)
    }

  def addKnowsRelation(
      aId1: Long,
      aId2: Long,
      movieId: Long
  ): Future[Unit] =
    driver.writeSession { session =>
      c"""
        MATCH (a1: Actor {tmdbId: $aId1})
        MATCH (a2: Actor {tmdbId: $aId2})
        MERGE (a1)-[r:KNOWS {movieId: $movieId}]-(a2)
      """.query[Unit].execute(session)
    }

  def addMoviesGenres(movieId: Long, genreId: Long): Future[Unit] =
    driver.writeSession { session =>
      c"""
        MATCH (m: Movie {tmdbId: ${movieId}})
        MATCH (g: Genre {tmdbId: ${genreId}})
        MERGE (m)-[r:BELONGS_TO]->(g)
      """.query[Unit].execute(session)
    }

  def addMoviesCountries(movieId: Long, iso_3166_1: String): Future[Unit] =
    driver.writeSession { session =>
      c"""
        MATCH (m: Movie {tmdbId: ${movieId}})
        MATCH (c: Country {iso_3166_1: ${iso_3166_1}})
        MERGE (m)-[r:PRODUCED_IN]->(c)
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

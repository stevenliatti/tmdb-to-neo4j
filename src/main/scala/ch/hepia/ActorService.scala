package ch.hepia

import ch.hepia.Domain.{Actor}
import neotypes.Driver
import neotypes.implicits._
import spray.json.JsonParser

import scala.concurrent.Future
import scala.io.Source

class ActorService(driver: Driver[Future]) {

  def createConstraints(): Future[Unit] =
    driver.readSession { session =>
      c"""
      CREATE CONSTRAINT ON (a:Actor) ASSERT a.id IS UNIQUE;
    """.query[Unit].execute(session)
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

  def readActorsFromFile(path: String): Iterator[Actor] = {
    import JsonFormats._

    Source
      .fromFile(path)
      .getLines
      .map(line => JsonParser(line).convertTo[Actor])
  }

}

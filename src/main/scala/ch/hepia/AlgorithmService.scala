/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

import neotypes.Driver
import neotypes.implicits._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Service to execute Neo4j algorithms on added data
  * @param driver neo4j scala driver
  */
class AlgorithmService(driver: Driver[Future]) {

  /**
    * Add knowsDegree property at Actor
    * for centrality computation
    * @return
    */
  def centrality(): Future[Unit] =
    driver.readSession { session =>
      c"""
      CALL gds.alpha.degree.write({
        nodeProjection: 'Actor',
        relationshipProjection: {
          KNOWS: {
            type: 'KNOWS',
            projection: 'REVERSE'
          }
        },
        writeProperty: 'knowsDegree'
      });
    """.query[Unit].execute(session)
    }

  /**
    * Add some degree attributes to genres
    * @return
    */
  def genreDegree(): Future[Unit] =
    driver.readSession { session =>
      c"""
      MATCH (g:Genre)
      SET g.belongsToDegree = size((g)<-[:BELONGS_TO]-())
      SET g.knownForDegree = size((g)<-[:KNOWN_FOR]-())
      SET g.degree = size((g)<-[:BELONGS_TO|:KNOWN_FOR]-());
    """.query[Unit].execute(session)
    }

  // /**
  //   * Add community to actors
  //   * @return
  //   */
  // def communities(): Future[Unit] =
  //   driver.readSession { session =>
  //     val a = c"""
  //     CALL gds.graph.create(
  //       'actor-knows-community-louvain-graph',
  //       'Actor',
  //       {
  //         KNOWS: {
  //           orientation: 'UNDIRECTED'
  //         }
  //       },
  //       {
  //         relationshipProperties: 'count'
  //       }
  //     );
  //   """.query[Unit].execute(session)
  //     Await.result(a, Duration.Inf)
  //     c"""
  //     CALL gds.louvain.write(
  //       'actor-knows-community-louvain-graph',
  //       { writeProperty: 'knowsCommunity' }
  //     ) YIELD communityCount, modularity, modularities;
  //   """.query[Unit].execute(session)
  //   }

  /**
    * Add relationships of similarity for movies and actors
    * @return
    */
  def similarities(): Future[Unit] =
    driver.readSession { session =>
      val a = c"""
      CALL gds.graph.create('movie-belongs-to-node-similar', ['Movie', 'Genre'], 'BELONGS_TO');
    """.query[Unit].execute(session)
      Await.result(a, Duration.Inf)
      val b = c"""
      CALL gds.nodeSimilarity.write('movie-belongs-to-node-similar', {
          writeRelationshipType: 'SIMILAR_MOVIES_ALGO',
          writeProperty: 'score'
      }) YIELD nodesCompared, relationshipsWritten;
    """.query[Unit].execute(session)
      Await.result(b, Duration.Inf)

      val c = c"""
      CALL gds.graph.create('actor-known-for-acting-node-similar', ['Actor', 'Genre'], 'KNOWN_FOR');
    """.query[Unit].execute(session)
      Await.result(c, Duration.Inf)
      c"""
      CALL gds.nodeSimilarity.write('actor-known-for-acting-node-similar', {
          writeRelationshipType: 'SIMILAR_FOR_ACTING',
          writeProperty: 'score'
      }) YIELD nodesCompared, relationshipsWritten;
    """.query[Unit].execute(session)
    }
}

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

/**
  * Service to execute Neo4j algorithms on added data
  * @param driver neo4j scala driver
  */
class AlgorithmService(driver: Driver[Future]) {

  /**
    * Compute some degrees
    * @return
    */
  def degrees() = {
    def actorDegree(): Future[Unit] =
      driver.writeSession { session =>
        c"""
          MATCH (a:Actor)
          SET a.knowsDegree = size((a)-[:KNOWS]-())
          SET a.playInDegree = size((a)-[:PLAY_IN]->())
          SET a.degree = size((a)-[:KNOWS|:PLAY_IN]-());
        """.query[Unit].execute(session)
      }

    def genreDegree(): Future[Unit] =
      driver.writeSession { session =>
        c"""
          MATCH (g:Genre)
          SET g.belongsToDegree = size((g)<-[:BELONGS_TO]-())
          SET g.knownForDegree = size((g)<-[:KNOWN_FOR]-())
          SET g.degree = size((g)<-[:BELONGS_TO|:KNOWN_FOR]-());
        """.query[Unit].execute(session)
      }

    def countryDegree(): Future[Unit] =
      driver.writeSession { session =>
        c"""
          MATCH (c:Country)
          SET c.degree = size((c)<--());
        """.query[Unit].execute(session)
      }

    actorDegree()
    genreDegree()
    countryDegree()
  }

  def communities() = {
    def knowsCommunityLouvain(): Future[Unit] =
      driver.writeSession { session =>
        c"""
          CALL gds.louvain.write(
            'actor-knows-communities',
            {
              writeProperty: 'knowsCommunityLouvain',
              relationshipWeightProperty: 'count'
            }
          ) YIELD communityCount, modularity, modularities;
        """.query[Unit].execute(session)
      }

    def knowsCommunityLabelPropagation() =
      driver.writeSession { session =>
        c"""
          CALL gds.labelPropagation.write(
            'actor-knows-communities',
            {
              writeProperty: 'knowsCommunityLabelPropagation',
              relationshipWeightProperty: 'count'
            }
          ) YIELD communityCount, ranIterations, didConverge
        """.query[Unit].execute(session)
      }

    def knowsCommunityModularityOptimization() =
      driver.writeSession { session =>
        c"""
          CALL gds.beta.modularityOptimization.write(
            'actor-knows-communities',
            {
              writeProperty: 'knowsCommunityModularityOptimization',
              relationshipWeightProperty: 'count'
            }
          ) YIELD modularity
        """.query[Unit].execute(session)
      }

    driver.writeSession { session =>
      val subGraph =
        c"""
          CALL gds.graph.create(
            'actor-knows-communities',
            'Actor',
            {
              KNOWS_COUNT: {
                orientation: 'UNDIRECTED'
              }
            },
            {
              relationshipProperties: 'count'
            }
          )
        """.query[Unit].execute(session)
      Await.result(subGraph, Duration.Inf)

      knowsCommunityLouvain()
      knowsCommunityLabelPropagation()
      knowsCommunityModularityOptimization()
    }
  }

}

/**
 * Movie Score Parser
 * From JSON movies data, create Neo4j database with nodes
 * and relationships between Movies, Peoples and Genres
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
   * Create graphs and execute Page Rank with
   * similar and recommended movies as links
   * @return
   */
  def pagerank(): Future[Unit] = driver.readSession { session =>
    val a = c"""
      CALL gds.graph.create('pagerank-movie-similar', 'Movie', 'SIMILAR');
    """.query[Unit].execute(session)
    Await.result(a, Duration.Inf)
    val b = c"""
      CALL gds.pageRank.write('pagerank-movie-similar', {
        maxIterations: 20,
        dampingFactor: 0.85,
        writeProperty: 'pagerankSimilar'
      }) YIELD nodePropertiesWritten AS writtenProperties, ranIterations;
    """.query[Unit].execute(session)
    Await.result(b, Duration.Inf)
    val c = c"""
      CALL gds.graph.create('pagerank-movie-recommendations', 'Movie', 'RECOMMENDATIONS');
    """.query[Unit].execute(session)
    Await.result(c, Duration.Inf)
    c"""
      CALL gds.pageRank.write('pagerank-movie-recommendations', {
        maxIterations: 20,
        dampingFactor: 0.85,
        writeProperty: 'pagerankRecommendations'
      }) YIELD nodePropertiesWritten AS writtenProperties, ranIterations;
    """.query[Unit].execute(session)
  }

  /**
   * Add knowsDegree property at People
   * for centrality computation
   * @return
   */
  def centrality(): Future[Unit] = driver.readSession { session =>
    c"""
      CALL gds.alpha.degree.write({
        nodeProjection: 'People',
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
  def genreDegree(): Future[Unit] = driver.readSession { session =>
    c"""
      MATCH (g:Genre)
      SET g.belongsToDegree = size((g)<-[:BELONGS_TO]-())
      SET g.knownForActingDegree = size((g)<-[:KNOWN_FOR_ACTING]-())
      SET g.knownForWorkingDegree = size((g)<-[:KNOWN_FOR_WORKING]-())
      SET g.knownForDegree = size((g)<-[:KNOWN_FOR_WORKING|:KNOWN_FOR_ACTING]-())
      SET g.degree = size((g)<-[:BELONGS_TO|:KNOWN_FOR_WORKING|:KNOWN_FOR_ACTING]-());
    """.query[Unit].execute(session)
  }

  /**
   * Add community to peoples
   * @return
   */
  def communities(): Future[Unit] = driver.readSession { session =>
    val a = c"""
      CALL gds.graph.create(
        'people-knows-community-louvain-graph',
        'People',
        {
          KNOWS: {
            orientation: 'UNDIRECTED'
          }
        },
        {
          relationshipProperties: 'count'
        }
      );
    """.query[Unit].execute(session)
    Await.result(a, Duration.Inf)
    c"""
      CALL gds.louvain.write(
        'people-knows-community-louvain-graph',
        { writeProperty: 'knowsCommunity' }
      ) YIELD communityCount, modularity, modularities;
    """.query[Unit].execute(session)
  }

  /**
   * Add relationships of similarity for movies and peoples
   * @return
   */
  def similarities(): Future[Unit] = driver.readSession { session =>
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
      CALL gds.graph.create('people-known-for-acting-node-similar', ['People', 'Genre'], 'KNOWN_FOR_ACTING');
    """.query[Unit].execute(session)
    Await.result(c, Duration.Inf)
    val d = c"""
      CALL gds.nodeSimilarity.write('people-known-for-acting-node-similar', {
          writeRelationshipType: 'SIMILAR_FOR_ACTING',
          writeProperty: 'score'
      }) YIELD nodesCompared, relationshipsWritten;
    """.query[Unit].execute(session)
    Await.result(d, Duration.Inf)

    val e = c"""
      CALL gds.graph.create('people-known-for-working-node-similar', ['People', 'Genre'], 'KNOWN_FOR_WORKING');
    """.query[Unit].execute(session)
    Await.result(e, Duration.Inf)
    c"""
      CALL gds.nodeSimilarity.write('people-known-for-working-node-similar', {
          writeRelationshipType: 'SIMILAR_FOR_WORKING',
          writeProperty: 'score'
      }) YIELD nodesCompared, relationshipsWritten;
    """.query[Unit].execute(session)
  }
}

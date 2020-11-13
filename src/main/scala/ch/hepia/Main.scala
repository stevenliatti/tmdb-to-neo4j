/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

import java.util.concurrent.TimeUnit

import ch.hepia.Domain._
import neotypes.implicits._
import org.neo4j.driver.v1.{AuthTokens, Config, GraphDatabase}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Program entry point
  */
object Main {
  def main(args: Array[String]): Unit = {
    val neo4jConfig = Config
      .build()
      .withEncryption()
      .withConnectionTimeout(60, TimeUnit.SECONDS)
      .withMaxConnectionLifetime(2, TimeUnit.HOURS)
      .withMaxConnectionPoolSize(200)
      .withConnectionAcquisitionTimeout(48, TimeUnit.HOURS)
      .toConfig

    val (neo4jHost, neo4jUser, neo4jPassword) = (
      sys.env.get("NEO4J_HOST"),
      sys.env.get("NEO4J_USER"),
      sys.env.get("NEO4J_PASSWORD")
    )
    val driver = (neo4jHost, neo4jUser, neo4jPassword) match {
      case (Some(host), Some(user), Some(password)) =>
        GraphDatabase.driver(
          host,
          AuthTokens.basic(user, password),
          neo4jConfig
        )
      case _ =>
        println("You have to define env variables")
        sys.exit(42)
    }

    val insertionService = new InsertionService(driver.asScala[Future])
    def getTime = java.util.Calendar.getInstance.getTime()

    // ------------------------------------------------------------------------
    // First step, create constraints on Nodes types
    println(getTime + " First step, create constraints on Nodes types")
    insertionService.createConstraints()

    // ------------------------------------------------------------------------
    // Second step, read movies in JSON and make filters
    println(
      getTime + " Second step, read movies in JSON and make filters"
    )
    val rMovies = insertionService.readMoviesFromFile("data/movies.json").toList
    val rActors = insertionService.readActorsFromFile("data/actors.json").toList

    // ------------------------------------------------------------------------
    // Third step, create maps and sets for fast access
    println(
      getTime + " Third step, create maps and sets for fast access"
    )
    val actorsMap = mutable.Map[Long, Actor]()
    rActors.foreach(
      a =>
        { // Create map of actors, for each id we have the corresponding actor
          actorsMap.put(a.id, a)
        }
    )
    val moviesMap =
      rMovies.map(m => {
        val actors = m.credits.cast.filter(a => actorsMap.contains(a.id))
        val credits = Credits(actors)
        Movie(
          m.id,
          m.title,
          m.overview,
          m.budget,
          m.revenue,
          m.genres,
          credits,
          m.backdrop_path,
          m.poster_path,
          m.production_countries,
          m.release_date,
          m.runtime,
          m.tagline
        )
      })

    // track movie's genres, Genre node
    val genresSet = mutable.Set[Genre]()
    // map associating a movie's id to his actors list (PlayInMovie only), PLAY_IN relation
    val actorsInMovie = mutable.Map[Long, List[PlayInMovie]]()
    // map associating a movie's id to his genres list, BELONGS_TO relation
    val genresOfMovie = mutable.Map[Long, List[Genre]]()
    // map associating an actor's id to his genres. Count genres occurences, KNOWN_FOR relation
    val genresOfActor = mutable.Map[Long, mutable.Map[Genre, Int]]()

    case class PairIds(one: Long, another: Long) {
      val pair: Set[Long] = Set.apply(one, another)

      override def equals(obj: Any): Boolean =
        obj match {
          case that: PairIds => pair == that.pair
          case _             => false
        }
    }
    // map associating an actors pair with a movie ids list, KNOWS relation
    val relationsBetweenTwoActors = mutable.Map[PairIds, List[Long]]()

    moviesMap.foreach { movie =>
      val localMovieGenres = mutable.Set[Genre]()
      movie.genres.foreach { genre =>
        genresSet.add(genre)
        localMovieGenres.add(genre)
      }
      genresOfMovie.put(movie.id, localMovieGenres.toList)
      actorsInMovie.put(movie.id, movie.credits.cast)
      movie.credits.cast.foreach { actor =>
        localMovieGenres.foreach { genre =>
          if (genresOfActor.get(actor.id).contains(genre)) {
            val actualCount = genresOfActor.get(actor.id).get(genre)
            genresOfActor(actor.id).put(genre, actualCount + 1)
          } else {
            val newGenreMap = mutable.Map[Genre, Int]()
            newGenreMap.put(genre, 1)
            genresOfActor.put(actor.id, newGenreMap)
          }
        }
      }
    }

    actorsInMovie.foreach {
      case (movieId, actors) =>
        for {
          a1 <- actors
          a2 <- actors
        } yield {
          if (a1.id != a2.id) {
            val pair = PairIds(a1.id, a2.id)
            if (relationsBetweenTwoActors.contains(pair)) {
              val actualMovies = relationsBetweenTwoActors(pair)
              relationsBetweenTwoActors.put(pair, movieId :: actualMovies)
            } else {
              relationsBetweenTwoActors.put(pair, movieId :: Nil)
            }
          }
        }
    }

    // ------------------------------------------------------------------------
    // Fourth step, insert nodes in Neo4j
    println(
      getTime + " Fourth step, insert nodes in Neo4j"
    )
    // insert genres nodes
    val gnf =
      Future.sequence(genresSet.map(genre => insertionService.addGenres(genre)))
    // insert actors (without movies, done later in relations)
    val anf = Future.sequence(actorsMap.map {
      case (id, actor) => insertionService.addActor(actor)
    })
    // insert movies (without genres and actors, done later in relations)
    val mnf =
      Future.sequence(moviesMap.map(movie => insertionService.addMovie(movie)))

    Await.result(gnf, Duration.Inf)
    Await.result(anf, Duration.Inf)
    Await.result(mnf, Duration.Inf)

    // ------------------------------------------------------------------------
    // Fifth step, insert relations in Neo4j
    println(
      getTime + " Fifth step, insert relations in Neo4j"
    )
    // PLAY_IN relation
    val playIn = Future.sequence(actorsInMovie.flatMap {
      case (movieId, actors) =>
        actors.map { actor =>
          insertionService.addPlayInRelation(actor, movieId)
        }
    })
    // BELONGS_TO relation
    val belongsTo = Future.sequence(genresOfMovie.flatMap {
      case (movieId, genres) =>
        genres.map(genre => insertionService.addMoviesGenres(movieId, genre.id))
    })
    // KNOWN_FOR relation
    val knownFor = Future.sequence(genresOfActor.flatMap {
      case (actorId, genres) =>
        genres.map {
          case (genre, count) =>
            insertionService.addKnownForRelation(actorId, genre.id, count)
        }
    })
    // // KNOWS relation
    val knows = Future.sequence(relationsBetweenTwoActors.flatMap {
      case (pairIds, movieIds) =>
        movieIds.map(movieId =>
          insertionService.addKnowsRelation(
            pairIds.one,
            pairIds.another,
            movieId
          )
        )
    })

    Await.result(playIn, Duration.Inf)
    Await.result(belongsTo, Duration.Inf)
    Await.result(knows, Duration.Inf)

    // ------------------------------------------------------------------------
    // Sixth step, run some algorithms on all data
    println(
      getTime + " Sixth step, run some algorithms on all data"
    )
    val algorithmService = new AlgorithmService(driver.asScala[Future])
    // Await.result(algorithmService.pagerank(), Duration.Inf)
    Await.result(algorithmService.centrality(), Duration.Inf)
    Await.result(algorithmService.genreDegree(), Duration.Inf)
    // Await.result(algorithmService.communities(), Duration.Inf)
    Await.result(algorithmService.similarities(), Duration.Inf)

    driver.close()
    println(getTime + " End of main")
  }
}

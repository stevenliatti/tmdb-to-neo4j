/**
  * TMDb to Neo4j Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Actors and Genres
  * Jeremy Favre & Steven Liatti
  */

package ch.hepia

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.collection.parallel.CollectionConverters._
import ch.hepia.Domain._
import neotypes.Driver
import neotypes.GraphDatabase
import neotypes.implicits._
import org.neo4j.driver.AuthTokens

/**
  * Program entry point
  */
object Main {
  def main(args: Array[String]): Unit = {
    val neo4jHost = sys.env.get("NEO4J_HOST")
    val driver = neo4jHost match {
      case Some(host) => GraphDatabase.driver(host)
      case _ =>
        println("You have to define env variables")
        sys.exit(42)
    }

    val insertionService = new InsertionService(driver)
    def getTime = java.util.Calendar.getInstance.getTime()

    // ------------------------------------------------------------------------
    // First step, create constraints on Nodes types
    println(getTime + " First step, create constraints on Nodes types")
    insertionService.createConstraints()

    // ------------------------------------------------------------------------
    // Second step, read movies and actors in JSON and make filters
    println(
      getTime + " Second step, read movies and actors in JSON and make filters"
    )
    val rMovies =
      insertionService.readMoviesFromFile("data/movies.json").toList
    val rActors =
      insertionService.readActorsFromFile("data/actors.json").toList

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
    // track movie's productions countries, Country node
    val countriesSet = mutable.Set[ProductionCountries]()
    // map associating a movie's id to its production countries, PRODUCED_IN relation
    val countriesOfMovie = mutable.Map[Long, List[ProductionCountries]]()
    // map associating a movie's id to its actors list (PlayInMovie only), PLAY_IN relation
    val actorsInMovie = mutable.Map[Long, List[PlayInMovie]]()
    // map associating a movie's id to its genres list, BELONGS_TO relation
    val genresOfMovie = mutable.Map[Long, List[Genre]]()
    // map associating an actor's id to its genres. Count genres occurences, KNOWN_FOR relation
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
      val localMovieCountries = mutable.Set[ProductionCountries]()

      movie.genres.foreach { genre =>
        genresSet.add(genre)
        localMovieGenres.add(genre)
      }

      movie.production_countries match {
        case Some(countries) =>
          countries.foreach { country =>
            countriesSet.add(country)
            localMovieCountries.add(country)
          }
        case _ => ()
      }

      genresOfMovie.put(movie.id, localMovieGenres.toList)
      countriesOfMovie.put(movie.id, localMovieCountries.toList)
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
      Future.sequence(genresSet.map(genre => insertionService.addGenre(genre)))
    // insert countries nodes
    val cnf =
      Future.sequence(
        countriesSet.map(country => insertionService.addCountry(country))
      )
    // insert actors (without movies, done later in relations)
    val anf = Future.sequence(actorsMap.map {
      case (id, actor) => insertionService.addActor(actor)
    })
    // insert movies (without genres and actors, done later in relations)
    val mnf =
      Future.sequence(moviesMap.map(movie => insertionService.addMovie(movie)))

    Await.result(gnf, Duration.Inf)
    Await.result(cnf, Duration.Inf)
    Await.result(anf, Duration.Inf)
    Await.result(mnf, Duration.Inf)

    // ------------------------------------------------------------------------
    // Fifth step, insert relations in Neo4j
    println(
      getTime + " Fifth step, insert relations in Neo4j"
    )

    // PRODUCED_IN relation
    println(getTime + "\tPRODUCED_IN")
    val producedIn = Future.sequence(countriesOfMovie.flatMap {
      case (movieId, countries) =>
        countries.map(country =>
          insertionService.addMoviesCountries(movieId, country.iso_3166_1)
        )
    })
    Await.result(producedIn, Duration.Inf)

    // BELONGS_TO relation
    println(getTime + "\tBELONGS_TO")
    val belongsTo = Future.sequence(genresOfMovie.flatMap {
      case (movieId, genres) =>
        genres.map(genre => insertionService.addMoviesGenres(movieId, genre.id))
    })
    Await.result(belongsTo, Duration.Inf)

    // KNOWN_FOR relation
    println(getTime + "\tKNOWN_FOR")
    val knownFor = Future.sequence(genresOfActor.flatMap {
      case (actorId, genres) =>
        genres.map {
          case (genre, count) =>
            insertionService.addKnownForRelation(actorId, genre.id, count)
        }
    })
    Await.result(knownFor, Duration.Inf)

    // PLAY_IN relation
    println(getTime + "\tPLAY_IN")
    actorsInMovie.par.flatMap {
      case (movieId, actors) =>
        actors.map { actor =>
          val f = insertionService.addPlayInRelation(actor, movieId)
          Await.result(f, Duration.Inf)
        }
    }

    // KNOWS_COUNT relation
    println(getTime + "\tKNOWS_COUNT")
    for {
      (pairIds, movieIds) <- relationsBetweenTwoActors.par
    } yield {
      val f = insertionService.addKnowsCountRelation(
        pairIds.one,
        pairIds.another,
        movieIds.length
      )
      Await.result(f, Duration.Inf)
    }

    // KNOWS relation
    println(getTime + "\tKNOWS")
    for {
      (pairIds, movieIds) <- relationsBetweenTwoActors.par
      movieId <- movieIds
    } yield {
      val f = insertionService.addKnowsRelation(
        pairIds.one,
        pairIds.another,
        movieId
      )
      Await.result(f, Duration.Inf)
    }

    // ------------------------------------------------------------------------
    // Sixth step, run some algorithms on all data
    println(
      getTime + " Sixth step, run some algorithms on all data"
    )
    val algorithmService = new AlgorithmService(driver)
    Await.result(algorithmService.actorDegree(), Duration.Inf)
    Await.result(algorithmService.genreDegree(), Duration.Inf)
    Await.result(algorithmService.countryDegree(), Duration.Inf)
    Await.result(algorithmService.communities(), Duration.Inf)
    // Await.result(algorithmService.similarities(), Duration.Inf)

    driver.close
    println(getTime + " End of main")
  }
}

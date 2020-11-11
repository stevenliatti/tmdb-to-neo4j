/**
  * Movie Score Parser
  * From JSON movies data, create Neo4j database with nodes
  * and relationships between Movies, Peoples and Genres
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
    val actorsByMovie = if (args.length > 0) args(0).toInt else 30
    println(s"$actorsByMovie actorsByMovie")
    val jobsForMovie = List(
      "Director",
      "Writer",
      "Screenplay",
      "Producer",
      "Director of Photography",
      "Editor",
      "Composer",
      "Special Effects"
    )
    val movieMakerScoreDivisor = 2.0

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
    //////////////////////////////////////////############################## Depuis ici

    // First step, create constraints on Nodes types
    val movieService = new MovieService(driver.asScala[Future])
    val actorService = new ActorService(driver.asScala[Future])

    println(java.util.Calendar.getInstance.getTime + " Create constraints")
    movieService.createConstraints()
    actorService.createConstraints()

    // Second step, read movies from raw JSON data and keep just the necessary
    println(
      java.util.Calendar.getInstance.getTime + " Read movies in JSON and make filters"
    )
    val rMovies = movieService.readMoviesFromFile("data/movies.json").toList
    val rActors = actorService.readActorsFromFile("data/actors.json").toList

    val actorMap = mutable.Map[Long, Actor]()
    rActors.foreach(
      a => { // Creat map of actors, for each id we have the corresponding actor
        actorMap.put(a.id, a)
      }
    )

    val movies =
      rMovies.map(m => { // Filter actors with a order min in the movie
        val actors = m.credits.cast.filter(a => actorMap.contains(a.id))
        val credits = Credits(actors)
        Movie(
          m.id,
          m.title,
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

    // Insert all actors in the database
    println(
      java.util.Calendar.getInstance.getTime + " Start to add actors nodes"
    )
    val actorsInsertions =
      Future.sequence(rActors.map(a => actorService.addActor(a)))
    Await.result(actorsInsertions, Duration.Inf)

    case class KnownRelation(a1Id: Long, a2Id: Long, movieIds: List[Long])
    val peopleToPeople = mutable.Map[(Long, Long), KnownRelation]()
    val genresSet = mutable.Set[Genre]()

    movies.foreach { m =>
      val actors = m.credits.cast
      val genres = m.genres
      genres.foreach(g => genresSet.add(g))

      // Create relations between actors by his ID
      for {
        a1 <- actors
        a2 <- actors
      } yield {
        if (a1.id != a2.id) {
          if (peopleToPeople.contains((a1.id, a2.id))) {
            val movies: List[Long] = peopleToPeople((a1.id, a2.id)).movieIds
            val newRel = KnownRelation(a1.id, a2.id, m.id :: movies)
            peopleToPeople.put((a1.id, a2.id), newRel)
          } else {
            val newRel = KnownRelation(a1.id, a2.id, m.id :: Nil)
            peopleToPeople.put((a1.id, a2.id), newRel)
          }
        }
      }
    }

    // Iter on all relations and insert it
    val addRelations = peopleToPeople.flatMap {
      case (_, v) =>
        v.movieIds.map(movieId => {
          actorService.addKnowsRelation(
            v.a1Id,
            v.a2Id,
            movieId
          )
        })
    }

    val genresInsertions = Future.sequence(genresSet.map(g => movieService.addGenres(g)))
    val fAddRel = Future.sequence(addRelations)
    Await.result(genresInsertions, Duration.Inf)
    Await.result(fAddRel, Duration.Inf) // Wait for adding relations

    val moviesForActor = mutable.Map[Long, List[PlayInMovie]]()
    val genresForActor = mutable.Map[Long, List[Genre]]()
    // val peopleToPeople = mutable.Map[Long, List[Long]]()

    // TODO
    // rActors.foreach(actor => {
    //   val actorId = actor.id
    //   val movies = actor.movie_credits

    //   movies.foreach(movie => {
    //     if ()
    //   })

    //   if (moviesForActor.contains(actorId)) {
    //     moviesForActor.put(actorId, moviesForActor(actorId))
    //   }
    // })

    def processPeople(p: People, m: Movie, genres: List[Genre]): Unit = { // TODO: On peut virer ca non ? (la partie mfp et genres je sais pas si tu veux garder)
      val (label, score) = p match {
        case actor: Actor =>
          (
            "Actor",
            movieService.computeMovieScore(m) / (actor.order + 1).toDouble
          )
        case _ =>
          (
            "MovieMaker",
            movieService.computeMovieScore(m) / movieMakerScoreDivisor
          )
      }

      if (peoplesMap.contains(p.id)) peoplesMap(p.id)._2 += label
      else {
        val sp = SimplePeople(p.id, p.name, p.intToGender())
        val set = mutable.Set(label)
        peoplesMap.put(p.id, (sp, set))
      }

      val mfp = MovieForPeople(m.id, p, score)
      if (moviesForPeople.contains(p.id))
        moviesForPeople.put(p.id, mfp :: moviesForPeople(p.id))
      else moviesForPeople.put(p.id, mfp :: Nil)

      genres.foreach { g =>
        val gfp = GenreForPeople(g.id, p)
        if (genresForPeople.contains(p.id))
          genresForPeople.put(p.id, gfp :: genresForPeople(p.id))
        else genresForPeople.put(p.id, gfp :: Nil)
      }
    }

    // Third step, make maps for peoples and genres, to speed up making relations
    println(
      java.util.Calendar.getInstance.getTime + " Create maps from each movies"
    )
    movies.foreach { m =>
      val actors = m.credits.cast
      val genres = m.genres

      actors.foreach(a => processPeople(a, m, genres))
      movieMakers.foreach(mm => processPeople(mm, m, genres))

      val people = actors ::: movieMakers
      for {
        p1 <- people
        p2 <- people
      } yield {
        if (p1.id != p2.id) {
          if (peopleToPeople.contains(p1.id))
            peopleToPeople.put(p1.id, p2.id :: peopleToPeople(p1.id))
          else peopleToPeople.put(p1.id, p2.id :: Nil)
        }
      }
    }

    // Fourth step, add all nodes in neo4j
    println(
      java.util.Calendar.getInstance.getTime + " Start to add movies, genres and peoples nodes"
    )
    val moviesInsertions =
      Future.sequence(movies.map(m => movieService.addMovie(m)))
    val peoplesInsertions = Future.sequence(peoplesMap.map {
      case (id, (sp, set)) =>
        val score =
          moviesForPeople(id).map(mfp => mfp.score).sum / moviesForPeople(
            id
          ).length
        movieService.addPeople(sp, score, set)
    })

    Await.result(moviesInsertions, Duration.Inf)
    Await.result(peoplesInsertions, Duration.Inf)

    // Fifth step, add relations
    println(
      java.util.Calendar.getInstance.getTime + " Start to make relations between all nodes"
    )
    // Add similar movies for each movie
    val similar = for {
      m1 <- movies
      m2 <- m1.similar.getOrElse(Similar(Nil)).results
    } yield movieService.addSimilarRelation(m1, m2)

    // Add recommended movies for each movie
    val recommendations = for {
      m1 <- movies
      m2 <- m1.recommendations.getOrElse(Recommendations(Nil)).results
    } yield movieService.addRecommendationsRelation(m1, m2)

    // Add genres for each movie
    val moviesGenres = for {
      m <- movies
      g <- m.genres
    } yield movieService.addMoviesGenres(m, g)

    val fSimilar = Future.sequence(similar)
    val fRecommendations = Future.sequence(recommendations)
    val fMoviesGenres = Future.sequence(moviesGenres)

    Await.result(fSimilar, Duration.Inf)
    println(
      java.util.Calendar.getInstance.getTime + " Similar relations for movies added"
    )
    Await.result(fRecommendations, Duration.Inf)
    println(
      java.util.Calendar.getInstance.getTime + " Recommended relations for movies added"
    )
    Await.result(fMoviesGenres, Duration.Inf)
    println(
      java.util.Calendar.getInstance.getTime + " Genres relations for movies added"
    )

    // Add add "in" relation between each people and movie
    val peoplesMovies = for {
      (_, moviesList) <- moviesForPeople
      m <- moviesList
    } yield movieService.addInMoviesRelation(m.people, m.movieId)

    val fPeoplesMovies = Future.sequence(peoplesMovies)
    Await.result(fPeoplesMovies, Duration.Inf)
    println(
      java.util.Calendar.getInstance.getTime + " Movies relations for peoples added"
    )

    val knownForRelations = genresForPeople.map {
      case (peopleId, gfpList) =>
        def genresPeopleCount(
            gfpList: List[GenreForPeople],
            kind: People
        ): Map[Long, (People, Int)] = {
          val knownFor = kind match {
            case _: Actor =>
              gfpList.filter(gfp => gfp.people.isInstanceOf[Actor])
            case _: MovieMaker =>
              gfpList.filter(gfp => gfp.people.isInstanceOf[MovieMaker])
          }
          val genresFor = knownFor
            .groupBy(gfp => gfp.genreId)
            .map { case (l, peoples) => (l, peoples.map(p => p.people)) }
          genresFor
            .map { case (l, peoples) => (l, (peoples.head, peoples.length)) }
        }

        val a = Actor(1, "", 1, 1, "")
        val mm = MovieMaker(1, "", 1, "")
        val genresActingCount = genresPeopleCount(gfpList, a)
        val genresWorkingCount = genresPeopleCount(gfpList, mm)
        (peopleId, (genresActingCount, genresWorkingCount))
    }

    // Add acting for each people
    for {
      (_, (genresActingCount, _)) <- knownForRelations
      (genreId, (people, count)) <- genresActingCount
    } yield {
      val f = movieService.addKnownForRelation(people, genreId, count)
      Await.result(f, Duration.Inf)
    }
    println(
      java.util.Calendar.getInstance.getTime + " Genres acting relations for peoples added"
    )

    // Add working for each people
    for {
      (_, (_, genresWorkingCount)) <- knownForRelations
      (genreId, (people, count)) <- genresWorkingCount
    } yield {
      val f = movieService.addKnownForRelation(people, genreId, count)
      Await.result(f, Duration.Inf)
    }
    println(
      java.util.Calendar.getInstance.getTime + " Genres working relations for peoples added"
    )

    // Add knows between each people
    val knows = peopleToPeople.map {
      case (peopleId, pIds) =>
        val friendIdWithCount = pIds
          .groupBy(i => i)
          .map { case (l, longs) => (l, longs.length) }
        (peopleId, friendIdWithCount)
    }
    for {
      (p1, p2Count) <- knows
      (p2, count) <- p2Count
    } yield {
      val f = movieService.addKnowsRelation(p1, p2, count)
      Await.result(f, Duration.Inf)
    }
    println(
      java.util.Calendar.getInstance.getTime + " Knows relations for peoples added"
    )

    // Sixth step, run some algorithms on all data
    val algorithmService = new AlgorithmService(driver.asScala[Future])
    Await.result(algorithmService.pagerank(), Duration.Inf)
    Await.result(algorithmService.centrality(), Duration.Inf)
    Await.result(algorithmService.genreDegree(), Duration.Inf)
    Await.result(algorithmService.communities(), Duration.Inf)
    Await.result(algorithmService.similarities(), Duration.Inf)
    println(
      java.util.Calendar.getInstance.getTime + " Compute some algorithms done"
    )

    driver.close()
    println(java.util.Calendar.getInstance.getTime + " End of main")
  }
}

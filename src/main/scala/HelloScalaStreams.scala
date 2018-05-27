import akka.stream._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorSystem, Props, Terminated}
import akka.util.ByteString

import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, RoundRobinRoutingLogic, Router}

object HelloScalaStreams extends App {

  case class Work(message: String)

  class Worker extends Actor {
    override def receive: Receive = {
      case w: Work ⇒
        println(w.message)
    }
  }

  class Master extends Actor {

    var router: Router = {
      val routees = Vector.fill(5) {
        val r = context.actorOf(Props[Worker])
        context watch r
        ActorRefRoutee(r)
      }
      Router(BroadcastRoutingLogic(), routees)
    }

    def receive = {
      case w: Work ⇒
        router.route(w, sender())
      case Terminated(a) ⇒
        router = router.removeRoutee(a)
        val r = context.actorOf(Props[Worker])
        context watch r
        router = router.addRoutee(r)
    }
  }

  val system = ActorSystem("pingpong")

  val master = system.actorOf(Props[Master], "master")

  import system.dispatcher
  system.scheduler.schedule(0 millis, 500 millis) {
    master ! Work(System.nanoTime().toString)
  }



//  val source: Source[Int, NotUsed] = Source(1 to 100)
//
//  val done: Future[Done] = source.runForeach(i ⇒ println(i))
//
//  done.onComplete(_ ⇒ system.terminate())
}

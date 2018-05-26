import akka.stream._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.util.ByteString

import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

import HelloScalaStreams.done

object HelloScalaStreams extends App {
  println("hi")

  implicit val system: ActorSystem = ActorSystem("QuickStart")

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val source: Source[Int, NotUsed] = Source(1 to 100)

  val done: Future[Done] = source.runForeach(i ⇒ println(i))

  done.onComplete(_ ⇒ system.terminate())
}

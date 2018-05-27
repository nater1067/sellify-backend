import HelloScalaStreams.Worker
import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import org.joda.time.{DateTime => JodaDT}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.routing.{BroadcastRoutingLogic, Router}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import org.joda.time.format.DateTimeFormat
import spray.json.{DefaultJsonProtocol, RootJsonFormat, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

final case class SalesPipelineEvent(
  event_name: String,
  productId: Int,
  prospectId: String,
  additionalInfo: Map[String, String],
  reportedTime: Option[JodaDT],
  applicationTime: Option[JodaDT],
)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  private val elasticSearchDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
  implicit object JodaJsonFormat extends RootJsonFormat[JodaDT] {
    def write(c: JodaDT) =
      JsString(elasticSearchDateTimeFormat.print(c))

    def read(value: JsValue): JodaDT = value match {
      case JsString(dateString) =>
        elasticSearchDateTimeFormat.parseDateTime(dateString)
      case _ => deserializationError("Color expected")
    }
  }
  implicit val salesPipelineEventFormat: RootJsonFormat[SalesPipelineEvent] = jsonFormat6(SalesPipelineEvent)
}

object SalesPipelineEventWebServer extends JsonSupport {

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def logToElasticsearch(event: SalesPipelineEvent): Future[SalesPipelineEvent] = {
    val responseFuture = Marshal(event).to[RequestEntity] flatMap { entity =>
      val request = HttpRequest(
        method = HttpMethods.POST,
        uri = "http://localhost:9200/salespipelineevents/sales_pipeline_event",
        entity = entity
      )
      Http().singleRequest(request)
    }

    responseFuture
      .map(x => {
        println("success:" + Unmarshal(x.entity).to[String])
        event
      })
      .recover({
        case e: Throwable =>
          println("Error: " + e.getMessage)
          event
      })
  }


  class SSEActor extends Actor {
    override def receive: Receive = {
      case e: SalesPipelineEvent => {
        println(e)
      }
    }
  }

  class ElasticsearchLoggingActor extends Actor {
    override def receive: Receive = {
      case event: SalesPipelineEvent =>
        logToElasticsearch(event)
    }
  }

  class EventsActor extends Actor {
    // Logs all received events to elasticsearch
    private val loggingActor = context.actorOf(Props[ElasticsearchLoggingActor])

    // Routes to server sent events actors for real-time display in client applications
    var sseRouter: Router = {
      Router(BroadcastRoutingLogic(), Vector.empty)
    }

    // A list of sales pipeline events this has received
    var items: List[SalesPipelineEvent] = List.empty

    def receive: PartialFunction[Any, Unit] = {
      case event: SalesPipelineEvent =>
        println("got " + event.toString)
        items = items :+ event
        loggingActor ! event
        sseRouter.route(event, sender())
      case x: ActorRef ⇒
        println("adding routee")
        sseRouter = sseRouter.addRoutee(x)
      case Terminated(a) ⇒
        println("terminated")
        sseRouter = sseRouter.removeRoutee(a)
        val r = context.actorOf(Props[Worker])
        context watch r
        sseRouter = sseRouter.addRoutee(r)
    }
  }

  val eventsActor: ActorRef = system.actorOf(Props[EventsActor])

  def eventsRoute: Route = {
    import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._

    implicit val salesPipelineEventFormat: RootJsonFormat[SalesPipelineEvent] = jsonFormat6(SalesPipelineEvent)

    path("events") {
      get {
        respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          val (sseActor: ActorRef, sseSource: Source[ServerSentEvent, NotUsed]) =
            Source.actorRef[SalesPipelineEvent](10, akka.stream.OverflowStrategy.dropTail)
              .map(s => {
                println("new event: ", s.toJson.toString)
                ServerSentEvent(s.toJson.toString())
              })
              .keepAlive(1.second, () => ServerSentEvent.heartbeat)
              .recover {
                case t: RuntimeException ⇒ {
                  println(t.getMessage)
                  ServerSentEvent(JsObject(
                    "err" -> JsString(t.getMessage)
                  ).toString())
                }
              }
              .toMat(BroadcastHub.sink[ServerSentEvent])(Keep.both)
              .run()

          eventsActor ! sseActor

          complete(sseSource)
        }
      } ~
        post {
          entity(as[SalesPipelineEvent]) { event =>
            eventsActor ! event.copy(applicationTime = Some(JodaDT.now))
            respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
              complete("")
            }
          }
        }
    }
  }

  def main(args: Array[String]) {
    val route = eventsRoute

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")

    StdIn.readLine() // let it run until user presses return

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

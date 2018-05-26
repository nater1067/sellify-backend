import java.time.LocalTime

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.io.StdIn
import akka.stream.scaladsl.{Sink, Source}
import java.time.format.DateTimeFormatter.ISO_LOCAL_TIME

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.actor.ActorPublisher
import org.reactivestreams.Publisher
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

import spray.json._
import scala.util.{Failure, Success}

final case class SalesPipelineEvent(
  event_name: String,
  productId: Int,
  prospectId: String,
  additionalInfo: Map[String, String]
)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val salesPipelineEventFormat: RootJsonFormat[SalesPipelineEvent] = jsonFormat4(SalesPipelineEvent)
}


object SalesPipelineEventWebServer extends JsonSupport {

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
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

  val actorRef: ActorRef = system.actorOf(Props[ActorBasedSource])
  val pub: Publisher[SalesPipelineEvent] = ActorPublisher[SalesPipelineEvent](actorRef)

  val eventsSource: Source[SalesPipelineEvent, NotUsed] = Source.fromPublisher(pub)
    .map(event => {

      logToElasticsearch(event)

      event
    })

  def eventsRoute: Route = {
    import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._

    implicit val salesPipelineEventFormat: RootJsonFormat[SalesPipelineEvent] = jsonFormat4(SalesPipelineEvent)

    path("events") {
      get {
        respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          complete {
            eventsSource.map(event => {

              event.toJson
            })
              .map(x => {
                ServerSentEvent(x.toString)
              })
              .keepAlive(1.second, () => ServerSentEvent.heartbeat)
          }
        }
      } ~
      post {
        entity(as[SalesPipelineEvent]) { event =>
          respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
            complete {
              actorRef ! event

              "success`!"
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]) {


//    val route =
//      path("hello") {
//        get {
//          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
//        }
//      }

    val route = eventsRoute

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

class ActorBasedSource extends Actor with ActorPublisher[SalesPipelineEvent]{
  import akka.stream.actor.ActorPublisherMessage._
  var items:List[SalesPipelineEvent] = List.empty

  def receive: PartialFunction[Any, Unit] = {
    case s:SalesPipelineEvent =>
      println("got " + s.toString)
      if (totalDemand == 0)
        items = items :+ s
      else
        onNext(s)

    case Request(demand) =>
      if (demand > items.size){
        items foreach (onNext)
        items = List.empty
      }
      else{
        val (send, keep) = items.splitAt(demand.toInt)
        items = keep
        send foreach (onNext)
      }


    case other =>
      println(s"got other $other")
  }
}

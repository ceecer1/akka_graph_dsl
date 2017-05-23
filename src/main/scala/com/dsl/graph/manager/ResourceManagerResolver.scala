package com.dsl.graph.manager

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.dsl.graph.entities.Common._
import com.dsl.graph.entities.business.BusinessId
import com.dsl.graph.entities.person.PersonId
import com.dsl.graph.util.ShardDefinitions

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by shishir on 5/18/17.
  */
object ResourceManagerResolver {

  def props(implicit actorSystem: ActorSystem): Props = {
    Props(new ResourceManagerResolver(new ActorLookup()))
  }

}

/**
  * A ResourceManagerResolver is an actor for discovering ResourceManager actors
  * @param actorReference
  */
class ResourceManagerResolver(actorReference: ActorLookup) extends Actor with ActorLogging {


  implicit val ec = context.dispatcher
  implicit val timeout = Timeout(5 seconds)
  implicit val actorSystem = context.system


  def receive = {

    case envelope: Envelope =>

      val targetActor = actorReference.actorLookup(envelope.actorId)
      log.info(s"processing envelope request: $envelope with actor: $targetActor")
      targetActor forward envelope

    case request: RouteRequest =>
      val targetId = request.id
      val targetActor: ActorRef = actorReference.actorLookup(targetId)
      val targetRequest = request.request
      val message = Envelope(targetId, targetRequest)
      targetActor.tell(message, sender)

    case o: Object =>
      log.info(s"------------------------GOT UNHANDLED message, need not worry for now: ${o.getClass.getName}")
  }
}

/**
  * This is a delegate class that's used to determine the appropriate
  * target for messages.
  *
  * @param actorSystem  This is needed to make the cluster stuff work
  */
class ActorLookup(implicit actorSystem: ActorSystem) {

  val shards = ShardDefinitions.shards
  val businessManager = shards.businessManager.shardStart
  val personManager = shards.personManager.shardStart


  def actorLookup(iD: ID): ActorRef = {
    iD match {
      case business: BusinessId => businessManager
      case person: PersonId => personManager
    }
  }

}

case class RouteRequest(id: ID, request: Request)
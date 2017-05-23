package com.dsl.graph.manager

import akka.actor.{ActorLogging, ActorSystem, PoisonPill, Props, ReceiveTimeout}
import akka.cluster.sharding.ShardRegion.Passivate
import akka.persistence._
import akka.util.Timeout
import com.dsl.graph.entities.Common._
import com.dsl.graph.manager.ResourceManagerProtocol.DestroySelf
import com.dsl.graph.service.{BusinessService, PersonService, ResourceService}
import com.dsl.graph.util._

import scala.concurrent.duration._

/**
  * Created by shishir on 5/18/17.
  */
object ResourceManager {

  def personManager(implicit system:ActorSystem) = Props(new ResourceManager(new PersonService(system)))
  def businessManager(implicit system:ActorSystem) = Props(new ResourceManager(new BusinessService(system)))

}

/**
  * A ResourceManager is an actor for managing all interactions
  * with Resources.  It's primary functions are:
  * Create, Read, Update and Destruction.
  * @param resourceService The resourceService to associate with this ResourceManager
  */
class ResourceManager(var resourceService: ResourceService) extends PersistentActor
  with ActorLogging
  with PubSubManager {


  protected var persistentResource: Resource = null
  protected var snapshotMetaData: SnapshotMetadata = null
  implicit val timeout = Timeout(5 seconds)
  implicit val ec = context.dispatcher
  implicit val actorSystem = context.system
  var resourceId: Option[ID] = None
  context.setReceiveTimeout(120.seconds)

  override def persistenceId = s"${self.path.name}"

  context.setReceiveTimeout(2 minutes)

  def receiveRecover = {

    case SnapshotOffer(metadata, resourceSnapshot: Resource) =>

      log.info(s"Recovering ${resourceSnapshot.getClass.getSimpleName} resource, Id=${resourceSnapshot.id}")
      if (resourceSnapshot != null) {
        this.persistentResource = resourceSnapshot
      }else {
        log.error("received a null resourceSnapshot, bad things to com")
      }
      this.snapshotMetaData = metadata
      log.info(s"recovery complete for ${persistentResource.id}")
    case resource:Resource  =>
      this.persistentResource = resource
    case RecoveryCompleted =>
      if (persistentResource == null){
        log.error(s"recovery completed but persistentResource is null. My persistenceId is $persistenceId and my resourceId is $resourceId")

      }
      log.info(s"recovery completed")
  }


  wrappedReceive = {

    case request: Envelope =>
      log.debug(s"Received envelope=$request from sender=${sender()}")
      resourceId match {
        case None =>
          this.resourceId = Some(request.actorId)
          log.debug(s"Received an envelope request presumably for the first time.  Setting my resourceId=${request.actorId}")
        case Some(shard) =>
          log.debug(s"got shard: $shard")
      }

      self forward request.request



    case request: CreateResource =>
      log.debug(s"Received request=$request from sender = ${sender()}")

      //persistentResource = resourceService.addResource(persistentResource, request.resource)
      persistentResource = request.resource
      saveSnapshot(persistentResource)
      sender ! CreateResourceSuccess(persistentResource)



    case request: GetResource =>
      log.debug(s"Received request=$request from sender=${sender()}")
      if ((null == persistentResource) && (resourceId.isEmpty)) {

        log.error("failed to get resource because persistentResource is null and there is no resourceId ")
        sender ! GetResourceError(request.iD.getClass.toString, ErrorMessage(s"persistentResource is null. " +
          s"request is $request. My resourceId is: $resourceId. This should not happen. "))

      }else if (null == persistentResource && resourceId.isDefined){

        log.error(s"failed to get resource because persistentResource is null (resource id is $resourceId)")
        sender ! GetResourceError(request.iD.getClass.toString, ErrorMessage(s"persistentResource is null. " +
          s"request is $request. My resourceId is: $resourceId. This should not happen. "))
      }
      else if (persistentResource.id == request.iD) {
        sender() ! GetResourceSuccess(persistentResource)
      } else {
        sender() ! GetResourceError(request.iD.getClass.toString, ErrorMessage("Requested id does not match actual " +
          "resource id.  This should never happen"))
      }


    case request: GetResourceSuccess =>
      log.debug(s"Received request=$request from sender=${sender()}")


    case request: GetResourceError =>
      log.debug(s"Received request=$request from sender=${sender()}")
      // This is currently only in use to handle response from SubscribeToParent actions
      log.error(s"Unable to add myself as a subscriber to the parent of ${request.resource}")


    case request: DestroyResource =>
      log.debug(s"Received request=$request from sender=${sender()}")

      //The persistentResource is going to die, so remove its topic subscribers
      val subscribers = persistentResource.publishTopics
      subscribers.keySet.map {
        key => subscribers.get(key).map {
          subSet => subSet.foreach {
            entityResource =>
              val resourceResolver = context.actorOf(ResourceManagerResolver.props)
              //inform the subscriber to remove this persistentResource's published topics
              resourceResolver ! Envelope(entityResource, RemoveSubscriptionTopic(EdgesConfig.getRelationPath(key), persistentResource.id))
          }
        }
      }

      //remove its topic publishers
      val publishers = persistentResource.subscriptionTopics
      publishers.keySet.map {
        key => publishers.get(key).map {
          publisherSubSet => publisherSubSet.foreach {
            entityResource =>
              val resourceResolver = context.actorOf(ResourceManagerResolver.props)
              //inform the publisher to remove this persistentResource's subscribed topics
              resourceResolver ! Envelope(entityResource, RemovePublishTopic(key, persistentResource.id))
          }
        }
      }

      // And destroy our self
      self forward DestroySelf


    //update Metadata for entity
    case request: UpdateMetadata =>
      log.debug(s"Received request=$request from sender=${sender()}")
      this.persistentResource = resourceService.updateResource(persistentResource, request.metadata)
      saveSnapshot(this.persistentResource)
      sender ! UpdateMetadataSuccess(persistentResource)



    /**
      * Establish an edge from this resource/entity to target resource/entity
      * edge is a string value, e.g. relativeOf, worksAt, friendOf, valueOf etc.
      */
    case request: EstablishEdge =>
      log.debug(s"Received request=$request from sender=${sender()}")
      this.persistentResource = persistentResource.addPublishTopic(request.edgeLabel, request.target)
      saveSnapshot(this.persistentResource)

      val subscriberResolver = context.actorOf(ResourceManagerResolver.props)
      subscriberResolver ! Envelope(request.target, AddSubscriptionTopic(EdgesConfig.getRelationPath(request.edgeLabel), persistentResource.id))
      sender ! EstablishEdgeSuccess(persistentResource)

    case request: RemoveEdge =>
      log.debug(s"Received request=$request from sender=${sender()}")
      this.persistentResource = persistentResource.removePublishTopic(request.edgeLabel, request.target)
      saveSnapshot(this.persistentResource)

      val subscriberResolver = context.actorOf(ResourceManagerResolver.props)
      subscriberResolver ! Envelope(request.target, RemoveSubscriptionTopic(EdgesConfig.getRelationPath(request.edgeLabel), persistentResource.id))
      sender ! EstablishEdgeSuccess(persistentResource)


    case request: SaveSnapshotSuccess =>
      log.debug(s"Received request=$request from sender=${sender()}")
      this.snapshotMetaData = request.metadata

    case request: ReceiveTimeout =>
      log.debug(s"Received request=$request from sender=${sender()}")
      context.parent ! Passivate(stopMessage = 'stop)

    case 'stop =>
      context.stop(self)

    case DestroySelf =>
      log.debug(s"Received request=DestroySelf from sender=${sender()}")

      deleteSnapshot(snapshotMetaData.sequenceNr)

      sender ! DestroyResourceSuccess(persistentResource)
      self ! PoisonPill

  }


}

object ResourceManagerProtocol {

  case object DestroySelf

}
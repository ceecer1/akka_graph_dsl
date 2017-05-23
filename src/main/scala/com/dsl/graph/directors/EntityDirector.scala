package com.dsl.graph.directors

import java.util.UUID

import akka.actor.ActorLogging
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}
import akka.util.Timeout
import com.dsl.graph.directors.EntityDirectorProtocol._
import com.dsl.graph.entities.Common._
import com.dsl.graph.entities.business.{Business, BusinessId, BusinessMetadata}
import com.dsl.graph.entities.person.{Person, PersonId, PersonMetadata}
import com.dsl.graph.manager.ResourceManagerResolver
import com.dsl.graph.service.{BusinessService, PersonService}
import com.dsl.graph.util.StackableActor
import akka.pattern.ask
import scala.util.{Failure, Success}

/**
  * Created by shishir on 5/18/17.
  */
object EntityDirectorProtocol {

  case object GetPersons

  case object GetBusinesses

  final case class GetEntitiesSuccess(entities: Set[_ <: ID]) extends SuccessResponse

  final case class GetEntitiesFailure(error: ErrorResponse) extends FailureResponse

  trait Event

  final case class AddPerson(personId: PersonId, email: String) extends Event

  final case class RemovePerson(personId: PersonId) extends Event

  final case class AddBusiness(businessId: BusinessId, email: String) extends Event

  final case class RemoveBusiness(businessId: BusinessId) extends Event

  final case class FindPerson(email: String) extends Request

  final case class FindBusiness(email: String) extends Request

}


/**
  * A EntityDirector stores only the entities Id for lookups
  */
class EntityDirector extends PersistentActor with StackableActor with ActorLogging {

  import scala.concurrent.duration._

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = context.dispatcher
  implicit val actorSystem = context.system

  context.setReceiveTimeout(120.seconds)

  //to create empty resources from each services, this could be an overhead
  protected var personService = new PersonService(actorSystem)
  protected var businessService = new BusinessService(actorSystem)

  var entities: EntitiesById = EntitiesById()

  override def persistenceId = s"EntityDirector"

  def receiveRecover = {
    case evt: Event =>
      updateState(evt)
      log.debug("recovering event={}", evt)
    case SnapshotOffer(_, personsSnapshot: EntitiesById) =>
      log.debug("recovering snapshot={}", personsSnapshot)
      entities = personsSnapshot
  }


  //The entirety of this actor's state is doing add/remove operations for entities' id
  def updateState(event: Event) = {
    event match {
      case AddPerson(personId, email) =>
        log.info(s"adding person ${personId.resourceId}")
        val byPersonsId = entities.byPersonsId + (personId -> email)
        val byPersonsEmail = entities.byPersonsEmail + (email -> personId)
        entities = EntitiesById(byPersonsId, byPersonsEmail, entities.byBusinessId, entities.byBusinessEmail)

      case RemovePerson(personId) =>
        val personEmail = entities.byPersonsId.get(personId)
        personEmail foreach {
          emailId =>
            val byPersonsId = entities.byPersonsId - personId
            val byPersonsEmail = entities.byPersonsEmail - emailId
            entities = EntitiesById(byPersonsId, byPersonsEmail, entities.byBusinessId, entities.byBusinessEmail)
        }

      case AddBusiness(businessId, email) =>
        log.info(s"adding business ${businessId.resourceId}")
        val byBusinessId = entities.byBusinessId + (businessId -> email)
        val byBusinessEmail = entities.byBusinessEmail + (email -> businessId)
        entities = EntitiesById(entities.byPersonsId, entities.byPersonsEmail, byBusinessId, byBusinessEmail)

      case RemoveBusiness(businessId) =>
        val businessEmail = entities.byBusinessId.get(businessId)
        businessEmail foreach {
          emailId =>
            val byBusinessId = entities.byBusinessId - businessId
            val byBusinessEmail = entities.byBusinessEmail - emailId
            entities = EntitiesById(entities.byPersonsId, entities.byPersonsEmail, byBusinessId, byBusinessEmail)
        }
    }
  }

  wrappedReceive = {

    case envelope: Envelope =>
      self forward envelope.request

    /**
      * The EntityDirector actor handles creating new entities.
      * CreateNewResource message contains the Entity metadata
      */
    case request: CreateNewResource =>
      val requester = sender()

        request.metadata match {
          case personMetadata: PersonMetadata =>
            val resolver = context.actorOf(ResourceManagerResolver.props)
            //FIXME The new resourceId must not be passed in, this should be created by resourceService
            val personId = request.iD.asInstanceOf[PersonId]
            //val person = personService.createEmptyResource(UUID.randomUUID(), metadata = personMetadata)
            val person = Person(personId, metadata = personMetadata)
            val email = person.metadata.email
            val byPersonsId = entities.byPersonsId + (person.id -> email)
            val byPersonsEmail = entities.byPersonsEmail + (email -> person.id)
            entities = EntitiesById(byPersonsId, byPersonsEmail, entities.byBusinessId, entities.byBusinessEmail)
            persist(AddPerson(person.id, email))(updateState)
            log.debug(s"Forwarding Person create message to ResourceManagerResolver")
            resolver forward Envelope(person.id, CreateResource(person))


          case businessMetadata: BusinessMetadata =>
            val resolver = context.actorOf(ResourceManagerResolver.props)
            //FIXME The new resourceId must not be passed in, this should be created by resourceService
            val businessId = request.iD.asInstanceOf[BusinessId]
            //val business = businessService.createEmptyResource(UUID.randomUUID(), metadata = businessMetadata)
            val business = Business(businessId, metadata = businessMetadata)
            val email = business.metadata.email
            val byBusinessId = entities.byBusinessId + (business.id -> email)
            val byBusinessEmail = entities.byBusinessEmail + (email -> business.id)
            entities = EntitiesById(entities.byPersonsId, entities.byPersonsEmail, byBusinessId, byBusinessEmail)
            persist(AddBusiness(business.id, email))(updateState)
            log.debug(s"Forwarding Business create message to ResourceManagerResolver")
            resolver forward Envelope(business.id, CreateResource(business))

          case x => sender ! CreateResourceError("Unknown", ErrorMessage("Metadata doesn't match any existing"))
        }

    /**
      * Handles returning all the Person entities id, the entity id is just like : EntityName(UUID)
      */
    case GetPersons =>
      sender ! GetEntitiesSuccess(entities.byPersonsId.keySet)

    /**
      * Handles returning all Business entities
      */
    case GetBusinesses =>
      sender ! GetEntitiesSuccess(entities.byBusinessId.keySet)

    /**
      * Returns the respective resource by ID passed into the message GetResource(ID)
      */
    case request: GetResource =>
      request.iD match {
        case personId: PersonId =>
          if (entities.byPersonsId.contains(personId)) {
            val resourceResolver = context.actorOf(ResourceManagerResolver.props)
            resourceResolver forward Envelope(personId, request)
          } else {
            sender ! GetResourceError(Person.getClass.getSimpleName, ErrorMessage("Person Does not Exist"))
          }

        case businessId: BusinessId =>
          if (entities.byBusinessId.contains(businessId)) {
            val resourceResolver = context.actorOf(ResourceManagerResolver.props)
            resourceResolver forward Envelope(businessId, request)
          } else {
            sender ! GetResourceError(Business.getClass.getSimpleName, ErrorMessage("Business Does not Exist"))
          }
      }

    /**
      * Finds a person by email passed into this message
      */
    case request: FindPerson =>
      log.debug(s"processing FindPerson: $request")

      val email = request.email
      val personId = entities.byPersonsEmail.get(email)
      log.debug(s"got personId: $personId")
      personId match {
        case Some(id) =>
          context.actorOf(ResourceManagerResolver.props)  forward Envelope(id, GetResource(id))

        case None =>
          sender ! GetResourceNotFound(Person.getClass.getSimpleName, ErrorMessage(s"Person with email=$email"))
      }

    /**
      * Finds a business by email passed into this message
      */
    case request: FindBusiness =>
      log.debug(s"processing FindPerson: $request")

      val email = request.email
      val businessId = entities.byBusinessEmail.get(email)
      log.debug(s"got businessId: $businessId")
      businessId match {
        case Some(id) =>
          context.actorOf(ResourceManagerResolver.props)  forward Envelope(id, GetResource(id))

        case None =>
          sender ! GetResourceNotFound(Business.getClass.getSimpleName, ErrorMessage(s"Business with email=$email"))
      }

    /**
      * Destroys the persistent resource actor
      * Based on which type of entity Id is sent in this DestroyResource message
      */
    case request: DestroyResource =>

      val replyTo = sender()

      request.iD match {
        case personId: PersonId =>
          val matches = entities.byPersonsId filter {
            case (existingId, email) => existingId == personId
          }
          if(matches.size ==0) {
            replyTo ! DeleteResourceError("Person", ErrorMessage("Person does not exist"))
          }
          matches foreach {
            case (existingId, email) =>
              val entityResolver = context.actorOf(ResourceManagerResolver.props)
              val futureResponse = (entityResolver ? Envelope(existingId, request)).mapTo[Response]
              futureResponse onComplete {
                case Success(response) =>
                  response match {
                    case success: DestroyResourceSuccess =>
                      persist(RemovePerson(personId))(updateState)
                      saveSnapshot(entities)
                      replyTo ! success
                    case error: DeleteResourceError => replyTo ! error
                    case x => replyTo ! x

                  }
                case Failure(e) =>
                  replyTo ! Failure(e)
              }
          }

        case businessId: BusinessId =>
          val matches = entities.byBusinessId filter {
            case (existingId, email) => existingId == businessId
          }
          if(matches.size ==0) {
            replyTo ! DeleteResourceError("Business", ErrorMessage("Business does not exist"))
          }
          matches foreach {
            case (existingId, email) =>
              val entityResolver = context.actorOf(ResourceManagerResolver.props)
              val futureResponse = (entityResolver ? Envelope(existingId, request)).mapTo[Response]
              futureResponse onComplete {
                case Success(response) =>
                  response match {
                    case success: DestroyResourceSuccess =>
                      persist(RemoveBusiness(businessId))(updateState)
                      saveSnapshot(entities)
                      replyTo ! success
                    case error: DeleteResourceError => replyTo ! error
                    case x => replyTo ! x

                  }
                case Failure(e) =>
                  replyTo ! Failure(e)
              }
          }

        case x =>
          log.warning("received a request to destroy a resource that doesn't belong to me.")
          sender() ! DeleteResourceError(x.getClass.getSimpleName, ErrorMessage("Cannot destroy resource because " +
            "I don't own it."))
      }


    case saveSnapShotSuccess: SaveSnapshotSuccess =>

    case saveSnapShotFailure: SaveSnapshotFailure =>

    case x => log.info(s"receive unhandled message: $x")

  }


}

// Note: this redundant Maps exists to provide O(1) operations for entityId lookups by id or email
// It is a tradeoff in memory for performance
case class EntitiesById(byPersonsId: Map[PersonId, String] = Map.empty,
                        byPersonsEmail: Map[String, PersonId] = Map.empty,
                        byBusinessId: Map[BusinessId, String] = Map.empty,
                        byBusinessEmail: Map[String, BusinessId] = Map.empty)


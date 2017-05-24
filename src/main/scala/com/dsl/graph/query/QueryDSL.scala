package com.dsl.graph.query

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import com.dsl.graph.entities.Common.{ID, Resource, _}
import akka.pattern.ask
import akka.util.Timeout
import com.dsl.graph.entities.person.PersonId
import com.dsl.graph.manager.RouteRequest
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Created by shishir on 5/19/17.
  */
trait QueryDSL { self =>

  implicit val timeout: akka.util.Timeout = Timeout(2 seconds)

  /**
    * This method returns an entity/resource from cluster
    * type parameterised of type Resource, e.g. Person or Business
    */
  protected def getEntity[ResourceType <: Resource](request: GetResource)
                                                   (implicit resolver: ActorRef,
                                                    ec: ExecutionContext): Future[Either[ErrorMessage, ResourceType]] = {
    val routeRequest = RouteRequest(request.iD, request)
    val entityFuture = (resolver ? routeRequest).mapTo[Response]
    val entity = entityFuture.map {
      case success: GetResourceSuccess =>
        Right(success.resource.asInstanceOf[ResourceType])

      case error: GetResourceError => Left(ErrorMessage(error.message.error))
      case x => Left(ErrorMessage("Unknown Error"))
    }
    entity

  }

  /**
    * This method returns related entity Ids given a single entity/resource
    * e.g Given a Person entity/resource and relationship edge name, the method returns all related entities
    * e.g To make more clear, this method returns all the published and subscribed friends/relatives
    *
    * @param resource
    * @param edgeName Relationship name i.e. relativeOf, friendOf
    * @return
    */
  private def getInOutRelationsOfEntity(resource: Resource,
                                        edgeName: String): Either[ErrorMessage, Set[_ <: ID]] = {

    //subscription topics are incoming relationships
    val subscribedTopics = resource.subscriptionTopics

    //publishTopics are outgoing relationships
    val publishedTopics = resource.publishTopics

    val setValuesAddition = new MergeMonoid[Set[_ <: ID]] {
      def op(a1: Set[_ <: ID], a2: Set[_ <: ID]): Set[_ <: ID] = a1 ++ a2

      def zero: Set[_ <: ID] = Set()
    }

    val topicsMerger: MergeMonoid[Map[String, Set[_ <: ID]]] = mapMerger(setValuesAddition)

    val mergedTopics = topicsMerger.op(subscribedTopics, publishedTopics)

    val result = mergedTopics.get(edgeName) match {
      case Some(ids) => {
        Right(ids)
      }
      case None => Left(ErrorMessage(s"No given relation $edgeName found for this resource ${resource.id.resourceId}"))
    }
    result
  }

  /**
    * Outgoing relation is the publish topics of an entity/resource
    * e.g if an entity establishes a relation with another entity, then the initiator has this info in publish topics
    * and another entity has that info in subscribed topics
    *
    * @param resource
    * @param edgeName
    * @return
    */
  private def getOutgoingRelationsOfEntity(resource: Resource,
                                           edgeName: String): Either[ErrorMessage, Set[_ <: ID]] = {

    //publishTopics are outgoing relationships
    val publishedTopics = resource.publishTopics

    val result = publishedTopics.get(edgeName) match {
      case Some(ids) => {
        Right(ids)
      }
      case None => Left(ErrorMessage(s"No given relation $edgeName found for this resource ${resource.id.resourceId}"))
    }
    result
  }

  /**
    * Incoming relation is the subscribed topics of an entity/resource
    * e.g if an entity establishes a relation with another entity, then the initiator has this info in publish topics
    * and another entity has that info in subscribed topics
    *
    * @param resource
    * @param edgeName
    * @return
    */
  private def getIncomingRelationsOfEntity(resource: Resource,
                                           edgeName: String): Either[ErrorMessage, Set[_ <: ID]] = {

    //subscription topics are incoming relationships
    val subscribedTopics = resource.subscriptionTopics

    val result = subscribedTopics.get(edgeName) match {
      case Some(ids) => {
        Right(ids)
      }
      case None => Left(ErrorMessage(s"No given relation $edgeName found for this resource ${resource.id.resourceId}"))
    }
    result
  }

  /**
    * This method checks if an Entity/Resource has more than a given relationship limit
    * Incoming relation is the subscribed topics of an entity/resource
    * In case of entities like Business, when a Person joins as an employee, the Business entity has a subscribed topic
    * with relation name e.g. "employed" with corresponding Person Id
    * Use cases Get business with more than Z employees, Get Person with more than Z properties
    *
    * @param resource
    * @param edgeName Relationship name e.g. "employed"
    * @param limit    The limit to check
    * @return
    */
  private def getEntityWithIncomingEdgeLimit(resource: Resource,
                                             edgeName: String,
                                             limit: Int
                                            ): Either[ErrorMessage, Set[_ <: ID]] = {

    //subscription topics are incoming relationships
    val subscribedTopics = resource.subscriptionTopics

    val result = subscribedTopics.get(edgeName) match {
      case Some(ids) =>
        if (ids.size > limit)
          Right(Set(resource.id))
        else Left(ErrorMessage(s"No given relation $edgeName found for this resource ${resource.id.resourceId}"))
      case None => Left(ErrorMessage(s"No given relation $edgeName found for this resource ${resource.id.resourceId}"))
    }
    result
  }


  /**
    * Given a set of entitiesId of type ID, relationship edge name and limit, the method returns all related Ids
    * matching the relation edge name
    * Use case: List all business with more than Z employees, List all persons who has more than Z children
    *
    * @param entities set of Entity IDs
    * @param edgeName Relationship name
    * @param limit    The limit Int value
    * @tparam ResourceIDType
    * @tparam ResourceType
    * @return
    */
  protected def getEntitiesHavingEdgeLimit[ResourceIDType <: ID, ResourceType <: Resource](
                                                                                            entities: Set[ResourceIDType],
                                                                                            edgeName: String,
                                                                                            limit: => Int)
                                                                                          (implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val seqSet: Seq[Set[_ <: ID]] = Seq()
    val set: Set[ID] = Set()

    val intermediate = entities.map {
      entity =>
        val futureOptionResource = getEntity[ResourceType](GetResource(entity))
        val futureSeq = futureOptionResource.map {
          x => x.map(y => getEntityWithIncomingEdgeLimit(y, edgeName, limit))
        }.map(either => either.toSeq.flatMap(another => another.toSeq))
        futureSeq
    }
    val futSeq = Future.sequence(intermediate)

    val result = futSeq.map(s => s.foldLeft(seqSet)(_ ++ _)).map(s => s.foldLeft(set)(_ ++ _))

    result

  }

  /**
    * Given a set of entitiesId of type ID and relationship edge name, the method returns all related Ids
    * matching the relation edge name
    * e.g Here ResourceIDType can be PersonId or BusinessId
    * ResourceType can be Person or Business, in most cases these are inferred
    *
    * @param entities
    * @param edgeName
    * @tparam ResourceIDType
    * @tparam ResourceType
    * @return
    */
  protected def getBiDirectionalRelationshipOfEntities[ResourceIDType <: ID, ResourceType <: Resource](
                                                                                                        entities: Set[ResourceIDType],
                                                                                                        edgeName: String)
                                                                                                      (implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val seqSet: Seq[Set[_ <: ID]] = Seq()
    val set: Set[ID] = Set()

    val intermediate = entities.map {
      entity =>
        val futureOptionResource = getEntity[ResourceType](GetResource(entity))
        val futureSeq = futureOptionResource.map {
          x => x.map(y => getInOutRelationsOfEntity(y, edgeName))
        }.map(either => either.toSeq.flatMap(another => another.toSeq))
        futureSeq
    }
    val futSeq = Future.sequence(intermediate)

    val result = futSeq.map(s => (s.foldLeft(seqSet)(_ ++ _))).map(s => s.foldLeft(set)(_ ++ _))

    result

  }


  /**
    * Given a set of entitiesId of type ID and relationship edge name, the method returns all related Ids
    * matching the relation edge name
    * e.g Given business Ids set and relation "employed", it returns all the initiators who published "worksAt"
    * relationship to one or many of the business ids set
    * Giving example, if a Person initiates a publish topic "worksAt" to a Business, the Business has a subscribed
    * "employed" relation with the Person
    * ResourceType can be Person or Business
    *
    * @param entities set of ID
    * @param edgeName relationship name
    * @tparam ResourceIDType
    * @tparam ResourceType
    * @return
    */
  protected def getSubscribedRelationshipOfEntities[ResourceIDType <: ID, ResourceType <: Resource](
                                                                                                     entities: Set[ResourceIDType],
                                                                                                     edgeName: String)
                                                                                                   (implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val seqSet: Seq[Set[_ <: ID]] = Seq()
    val set: Set[ID] = Set()

    val intermediate = entities.map {
      entity =>
        val futureOptionResource = getEntity[ResourceType](GetResource(entity))
        val futureSeq = futureOptionResource.map {
          x => x.map(y => getIncomingRelationsOfEntity(y, edgeName))
        }.map(either => either.toSeq.flatMap(another => another.toSeq))
        futureSeq
    }
    val futSeq = Future.sequence(intermediate)

    val result = futSeq.map(s => (s.foldLeft(seqSet)(_ ++ _))).map(s => s.foldLeft(set)(_ ++ _))

    result

  }

  /**
    * Get bidirectional relations e.g. get all friends/relatives of person, his outgoing and incoming all
    * but this method may not work with Business, because relationship both ways is not same.
    *
    * @param id       id of the entity
    * @param edgeName Relationship name
    * @param resolver
    * @param ec
    * @tparam ResourceIDType
    * @tparam ResourceType
    * @return
    */
  protected def getBidirectionalRelationsOfAnEntity[ResourceIDType <: ID, ResourceType <: Resource](
                                                                                                     id: ResourceIDType,
                                                                                                     edgeName: String)
                                                                                                   (implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val futureOptionResource = getEntity[ResourceType](GetResource(id))

    val futureSeq = futureOptionResource.map {
      either => either.map(y => getInOutRelationsOfEntity(y, edgeName))
    }

    val futureIdSets = futureSeq.map(either => either.toSeq.flatMap(another => another.toSeq))
    val emptySet: Set[ID] = Set()
    val result = futureIdSets.map(s => (s.foldLeft(emptySet)(_ ++ _)))

    result

  }

  /**
    * This method returns all the entity ids matching subscribed topics of an entityId
    * e.g. Handy in finding get all persons who works at Business Y
    *
    * @param id       BusinessId
    * @param edgeName relationship name, e.g. in case of business, it can be "employed"
    * @param resolver ResourceManagerResolver actor reference
    * @param ec
    * @tparam ResourceIDType
    * @tparam ResourceType
    * @return
    */
  protected def getIncomingRelationsOfAnEntity[ResourceIDType <: ID, ResourceType <: Resource](
                                                                                                id: ResourceIDType,
                                                                                                edgeName: String)
                                                                                              (implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val futureOptionResource = getEntity[ResourceType](GetResource(id))

    val futureSeq = futureOptionResource.map {
      either => either.map(y => getIncomingRelationsOfEntity(y, edgeName))
    }

    val futureIdSets = futureSeq.map(either => either.toSeq.flatMap(another => another.toSeq))
    val emptySet: Set[ID] = Set()
    val result = futureIdSets.map(s => (s.foldLeft(emptySet)(_ ++ _)))

    result

  }

  /**
    * This method gives back the all relatives of an entity, may not use this
    *
    * @param id       The entity whose relatives is to be found
    * @param entities Pass All entities for checking
    * @param edgeName relationship name
    * @param resolver ResourceManagerResolver actor
    * @param ec       Execution Ctx
    * @tparam ResourceIDType of type ID
    * @tparam ResourceType   of type Resource
    * @return
    */
  protected def relationsOfEntity[ResourceIDType <: ID, ResourceType <: Resource](id: ResourceIDType, entities: Set[ResourceIDType],
                                                                                  edgeName: String)(implicit resolver: ActorRef, ec: ExecutionContext): Future[Set[ID]] = {

    val futureOptionResource = getEntity[ResourceType](GetResource(id))

    val futureSeq = futureOptionResource.map {
      either => either.map(y => getOutgoingRelationsOfEntity(y, edgeName))
    }

    val futureIdSets = futureSeq.map(either => either.toSeq.flatMap(another => another.toSeq))
    val emptySet: Set[ID] = Set()
    val idsPublished = futureIdSets.map(s => (s.foldLeft(emptySet)(_ ++ _)))

    val idsSubscribed = getSubscribedRelationshipOfEntities(entities, edgeName) //.map(set => set.filter(_.equals(id)))

    val result = for {
      pubIds <- idsPublished
      subIds <- idsSubscribed
    } yield pubIds ++ subIds

    result

  }


  /**
    * for merging subscription topics and publish topics in a safe way
    * e.g. Merging Map("relativeOf" -> Set(1, 2)) and Map("worksAt" -> Set(5, 6), "relativeOf" -> Set(7, 8))
    * gives back Map(relativeOf -> Set(1, 2, 7, 8), worksAt -> Set(5, 6))
    *
    * @param V
    * @tparam K
    * @tparam V
    * @return
    */
  private def mapMerger[K, V](V: MergeMonoid[V]): MergeMonoid[Map[K, V]] =
    new MergeMonoid[Map[K, V]] {
      def zero = Map[K, V]()

      def op(a: Map[K, V], b: Map[K, V]) =
        (a.keySet ++ b.keySet).foldLeft(zero) { (acc, k) =>
          acc.updated(k, V.op(a.getOrElse(k, V.zero),
            b.getOrElse(k, V.zero)))
        }
    }

  sealed trait MergeMonoid[A] {
    def op(a1: A, a2: A): A

    def zero: A
  }


  implicit class QueryDSLOps[ResourceIDType <: ID, ResourceType <: Resource](a: Future[Set[ID]])(implicit resolver: ActorRef,
                                                                                             ec: ExecutionContext) {
    def getBiDirectionalRelationshipOfEntities(edgeName: String): Future[Set[ID]] =
      a.flatMap(self.getBiDirectionalRelationshipOfEntities(_, edgeName))

    def getSubscribedRelationshipOfEntities(edgeName: String): Future[Set[ID]] =
      a.flatMap(self.getSubscribedRelationshipOfEntities(_, edgeName))

    def getEntitiesHavingEdgeLimit(edgeName: String, limit: Int): Future[Set[ID]] =
      a.flatMap(self.getEntitiesHavingEdgeLimit(_, edgeName, limit))

    def relationsOfEntity(id: ResourceIDType, edgeName: String): Future[Set[ID]] =
      a.flatMap(self.relationsOfEntity(id, _, edgeName))
  }

}


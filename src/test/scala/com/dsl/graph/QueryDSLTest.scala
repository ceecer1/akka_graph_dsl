package com.dsl.graph

import java.util.UUID

import akka.pattern.ask
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.dsl.graph.directors.EntityDirectorFactory
import com.dsl.graph.directors.EntityDirectorProtocol.{FindBusiness, GetEntitiesSuccess, GetPersons}
import com.dsl.graph.entities.Common._
import com.dsl.graph.entities.business.{Business, BusinessId, BusinessMetadata}
import com.dsl.graph.entities.person.{Person, PersonId, PersonMetadata}
import com.dsl.graph.manager.{ActorLookup, ResourceManagerResolver, TestConfig}
import com.dsl.graph.query.QueryDSL
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by shishir on 5/19/17.
  */
class QueryDSLTest extends TestKit(ActorSystem("akka_graph_dsl_test", TestConfig.config))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar
  with QueryDSL
  with ImplicitSender{

  implicit val ec = system.dispatcher
  implicit override val timeout = Timeout(10 seconds)
  EntityDirectorFactory.createEntityDirector(system)
  val actorReference = mock[ActorLookup]
  implicit val resolver = system.actorOf(ResourceManagerResolver.props)

  val p1 = PersonId(UUID.randomUUID())
  val p2 = PersonId(UUID.randomUUID())
  val p3 = PersonId(UUID.randomUUID())
  val p4 = PersonId(UUID.randomUUID())
  val p5 = PersonId(UUID.randomUUID())
  val p6 = PersonId(UUID.randomUUID())
  val p7 = PersonId(UUID.randomUUID())

  val b1 = BusinessId(UUID.randomUUID())
  val b2 = BusinessId(UUID.randomUUID())
  val b3 = BusinessId(UUID.randomUUID())

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The QueryDSLTest run" should {

    "create entities and establish relation between entities" in {

      val entityDirector = EntityDirectorFactory.entityDirector
      entityDirector ! CreateNewResource(p1, PersonMetadata("ccer", "ccer@ccer1.com"))
      entityDirector ! CreateNewResource(p2, PersonMetadata("stew", "stew@ccer1.com"))
      entityDirector ! CreateNewResource(p3, PersonMetadata("stephen", "stephen@ccer1.com"))
      entityDirector ! CreateNewResource(p4, PersonMetadata("neil", "neil@ccer1.com"))
      entityDirector ! CreateNewResource(p5, PersonMetadata("chris", "chris@ccer1.com"))
      entityDirector ! CreateNewResource(p6, PersonMetadata("anya", "anya@ccer1.com"))
      entityDirector ! CreateNewResource(p7, PersonMetadata("Edward", "edward@ccer1.com"))

      entityDirector ! CreateNewResource(b1, BusinessMetadata("Amazon", "amazon@amazon1.com"))
      entityDirector ! CreateNewResource(b2, BusinessMetadata("Facebook", "facebook@facebook1.com"))
      entityDirector ! CreateNewResource(b3, BusinessMetadata("Samsung", "samsung@samsung1.com"))

      Thread.sleep(3000)

      resolver ! Envelope(p1, EstablishEdge(p2, "relativeOf"))
      resolver ! Envelope(p2, EstablishEdge(p3, "friendOf"))
      resolver ! Envelope(p1, EstablishEdge(b1, "worksAt"))
      resolver ! Envelope(p2, EstablishEdge(b2, "worksAt"))
      resolver ! Envelope(p3, EstablishEdge(b3, "worksAt"))
      resolver ! Envelope(p1, EstablishEdge(p4, "relativeOf"))
      resolver ! Envelope(p1, EstablishEdge(p5, "relativeOf"))
      resolver ! Envelope(p2, EstablishEdge(p6, "friendOf"))
      resolver ! Envelope(p7, EstablishEdge(b2, "worksAt"))
      resolver ! Envelope(p6, EstablishEdge(p7, "kindOf"))

      Thread.sleep(3000)

    }

    "Find All relatives of Person(X)" in {
      val allRelativesOfPersonTest = getBidirectionalRelationsOfAnEntity(p1, "relativeOf")

      val idSet = Await.result(allRelativesOfPersonTest, 5 second)

      idSet.size should be(3)
    }

    "List the relatives of an every person who works at Business(Y)" in {
      val relativesOfPersonWhoWorksAtBusinessY = getIncomingRelationsOfAnEntity(b1, "employed")
                                  .flatMap(getBiDirectionalRelationshipOfEntities(_, "relativeOf"))


      val idSet = Await.result(relativesOfPersonWhoWorksAtBusinessY, 5 second)

      idSet.size should be(3)
    }

    "List all business with more than Z employees" in {
      val businessSet = Future(Set(b1, b2, b3))
      val allBusinessesMoreThanZEmployees = businessSet.flatMap(getEntitiesHavingEdgeLimit(_, "employed", 1))
      val idSet = Await.result(allBusinessesMoreThanZEmployees, 5 second)

      idSet.size should be(1)
    }

    "List every person who has friends with employed relatives" in {
      val businessSet = Future(Set(b1, b2, b3))
      val personsFriendsWithEmployedRelatives = businessSet.flatMap(getSubscribedRelationshipOfEntities(_, "employed"))
                                                      .flatMap(getBiDirectionalRelationshipOfEntities(_, "relativeOf"))
                                                      .flatMap(getBiDirectionalRelationshipOfEntities(_, "friendOf"))
      val idSet = Await.result(personsFriendsWithEmployedRelatives, 5 second)

      idSet.size should be(2)
    }

    "List every person who has kinds with friends with employed relatives" in {
      val businessSet = Future(Set(b1, b2, b3))
      val personsFriendsWithEmployedRelatives = businessSet.flatMap(getSubscribedRelationshipOfEntities(_, "employed")
                                                          .getBiDirectionalRelationshipOfEntities("relativeOf")
                                                          .getBiDirectionalRelationshipOfEntities("friendOf")
                                                          .getBiDirectionalRelationshipOfEntities("kindOf"))
      val idSet = Await.result(personsFriendsWithEmployedRelatives, 5 second)

      idSet.head shouldEqual(p7)
    }

    "add relation between two business entities" in {
      resolver ! Envelope(b1, EstablishEdge(b2, "parent"))
      Thread.sleep(1000)
      val getParentOfEntity = getIncomingRelationsOfAnEntity(b2, "child")
      val idSet = Await.result(getParentOfEntity, 5 second)
      idSet.head shouldEqual(b1)
    }

    "remove relation between two business entities" in {
      resolver ! Envelope(b1, RemoveEdge(b2, "parent"))
      Thread.sleep(1000)
      val getParentOfEntity = getIncomingRelationsOfAnEntity(b2, "child")
      val idSet = Await.result(getParentOfEntity, 5 second)
      idSet.size should be(0)
    }




  }

}

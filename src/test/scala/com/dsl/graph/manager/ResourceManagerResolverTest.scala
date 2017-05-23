package com.dsl.graph.manager

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit._
import com.dsl.graph.entities.Common._
import com.dsl.graph.entities.business.{Business, BusinessId, BusinessMetadata}
import com.dsl.graph.service.PersonService
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.duration._
/**
  * Created by shishir on 5/19/17.
  */
class ResourceManagerResolverTest extends TestKit(ActorSystem("akka_graph_dsl_test", TestConfig.config))
  with DefaultTimeout
  with ImplicitSender with FlatSpecLike with MockitoSugar with BeforeAndAfterAll {

  "A resource manager resolver" should
    "handle a route request" in new Base {

    val routeRequest = RouteRequest(businessId, getBusiness)

    actorUnderTest ! routeRequest
    businessManagerProbe.expectMsg(Envelope(businessId, getBusiness))
    businessManagerProbe.reply(GetResourceSuccess(business))

  }

  override def afterAll(): Unit = {
    shutdown()

  }


}

abstract class Base(implicit actorSystem: ActorSystem) extends MockitoSugar {

  val actorReference = mock[ActorLookup]
  val actorUnderTest = TestActorRef(new ResourceManagerResolver(actorReference))

  val businessId = BusinessId(resourceId = UUID.randomUUID())
  val getBusiness = GetResource(businessId)

  val businessMetadata = BusinessMetadata(name = "Apple Computers", email = "apple@apple.com")
  val business = Business(businessId, businessMetadata)

  val businessManagerProbe = TestProbe()

  when(actorReference.actorLookup(businessId)) thenReturn businessManagerProbe.testActor

}

object TestConfig {
  val hostname = "127.0.0.1"
  val port = "3551"
  val systemName = "akka_graph_dsl_test"
  def config = {

    ConfigFactory.parseString(
      s"""
         |akka.loglevel=WARNING
         |akka {
         |  loggers = ["akka.event.slf4j.Slf4jLogger"]
         |  loglevel = INFO
         |}
         |akka.actor{
         |  provider="akka.cluster.ClusterActorRefProvider"
         |  warn-about-java-serializer-usage = false
         |}
         |akka.remote.netty.tcp.hostname="$hostname"
         |akka.remote.netty.tcp.port="$port"
         |akka.cluster.seed-nodes=["akka.tcp://$systemName@$hostname:$port"]
         |akka {
         |  persistence {
         |    journal.plugin = "inmemory-journal"
         |    snapshot-store.plugin = "inmemory-snapshot-store"
         |  }
         |}
         |""".stripMargin)

  }


}

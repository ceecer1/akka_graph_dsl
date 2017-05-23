package com.dsl.graph.util

import akka.actor.{ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.dsl.graph.entities.Common.{Envelope, Request}

/**
  * Created by shishir on 5/18/17.
  */
class ShardableResource(numberOfShards: Int, shardType: String, actorProps: Props, implicit val actorSystem: ActorSystem) {

  val idExtractor: ShardRegion.ExtractEntityId = {

    case requestEnvelope: Envelope => {
      val id = requestEnvelope.actorId
      val uuid = id.resourceId
      val extractedId = (uuid).toString
      (extractedId, requestEnvelope) // idExtractor transforms the Envelope into tuple containing
      // the identity of the shard and the message to be sent to it
    }
  }

  //shardResolver returns the shard id, then akka checks which node is it in. If correct, evaluate idExtractor
  val shardResolver: ShardRegion.ExtractShardId = {
    case requestEnvelope: Envelope => {
      val id = math.abs(requestEnvelope.actorId.resourceId.getMostSignificantBits % 30)
      (id.toString)
    }
  }

  val shardStart = ClusterSharding(actorSystem).start(shardType, actorProps,
      ClusterShardingSettings(actorSystem), idExtractor, shardResolver)

  val shardRef = ClusterSharding(actorSystem).shardRegion(shardType)

  def extractRequest(requestEnvelope: Envelope): Request = {
    requestEnvelope.request match {
      case envelope: Envelope => extractRequest(envelope)
      case x => requestEnvelope.request
    }
  }

}

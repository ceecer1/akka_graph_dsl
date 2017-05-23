package com.dsl.graph.entities

import java.util.UUID

/**
  * Created by shishir on 5/18/17.
  */
object Common {

  class ID(val resourceId: UUID) extends Serializable {

    def canEqual(other: Any): Boolean = other.isInstanceOf[ID]

    override def equals(other: Any): Boolean = other match {
      case that: ID =>
        (that canEqual this) &&
          resourceId == that.resourceId
      case _ => false
    }

    override def hashCode(): Int = {
      resourceId.hashCode()
    }

    override def toString = s"ID($resourceId)"


  }

  trait Resource extends Serializable {
    def id: ID

    def metadata: Metadata

    def addSubscriptionTopic(edge: String, id: ID): Resource

    def removeSubscriptionTopic(edge: String, id: ID): Resource

    def subscriptionTopics: Map[String, Set[_ <: ID]]

    def removePublishTopic(edge: String, id: ID): Resource

    def addPublishTopic(edge: String, id: ID): Resource

    def publishTopics: Map[String, Set[_ <: ID]]
  }

  trait Metadata

  trait Request

  trait Response

  trait SuccessResponse extends Response

  trait FailureResponse extends Response

  trait ErrorResponse extends Response

  case class ErrorMessage(error: String) extends ErrorResponse

  final case class CreateNewResource(iD: ID, metadata: Metadata) extends Request

  final case class CreateResource(resource: Resource) extends Request

  final case class CreateResourceSuccess(resource: Resource) extends SuccessResponse

  final case class CreateResourceError(resource: String, message: ErrorMessage) extends ErrorResponse

  final case class GetResource(iD: ID) extends Request

  final case class GetResourceSuccess(resource: Resource) extends SuccessResponse

  final case class GetResourceError(resource: String, message: ErrorMessage) extends ErrorResponse

  final case class GetResourceNotFound(resource:String, message:ErrorMessage) extends ErrorResponse

  final case class DestroyResource(iD: ID) extends Request

  final case class DestroyResourceSuccess(resource: Resource) extends SuccessResponse

  final case class DeleteResourceError(resource: String, message: ErrorMessage) extends ErrorResponse

  final case class UpdateMetadata(metadata: Metadata) extends Request

  final case class UpdateMetadataSuccess(resource: Resource) extends SuccessResponse

  final case class ResourceMetadataUpdateError(resource: String, message: ErrorMessage) extends ErrorResponse

  final case class EstablishEdge(target: ID, edgeLabel: String) extends Request

  final case class EstablishEdgeSuccess(resource: Resource) extends SuccessResponse

  final case class RemoveEdge(target: ID, edgeLabel: String) extends Request

  final case class RemoveEdgeSuccess(resource: Resource) extends SuccessResponse

  final case class Envelope(actorId: ID, request: Request) extends Request

}

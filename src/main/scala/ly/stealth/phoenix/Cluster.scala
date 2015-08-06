package ly.stealth.phoenix

import org.apache.mesos.Protos.Offer

import scala.collection.mutable
import scala.collection.JavaConverters._

object ResourceOffer {
  def fromMesosOffer(offer: Offer): ResourceOffer = {
    val offerResources = offer.getResourcesList.asScala.toList.map(res => res.getName -> res).toMap

    val cpus = offerResources.get("cpus").map(_.getScalar.getValue)
    val mem = offerResources.get("mem").map(_.getScalar.getValue)

    ResourceOffer(cpus, mem)
  }
}

case class ResourceOffer(cpus: Option[Double], mem: Option[Double]) {

  private def matchDoubles(requested: Option[Double], offered: Option[Double]) =
    requested.forall(r => offered.exists(o => o >= r))

  def matches(requestTemplate: RequestTemplate): Boolean = {
    matchDoubles(requestTemplate.cpus, cpus) &&
      matchDoubles(requestTemplate.mem, mem)
  }
}

case class RequestTemplate(cpus: Option[Double], mem: Option[Double])

object State {
  val STOPPED = "stopped"
  val STARTING = "starting"
  val RUNNING = "running"
  val RECONCILING = "reconciling"
  val STOPPING = "stopping"
}

case class Node(taskId: String, slaveId: String,
                executorId: String, hostname: String,
                attributes: Map[String, String], var state: String)

class Cluster {

  @volatile var plan = mutable.Map.empty[RequestTemplate, Option[Node]]

  def getNode(taskId: String): Option[Node] = {
    plan.values.flatten.find {
      node => node.taskId == taskId
    }
  }

  def requestSatisfied(request: RequestTemplate, node: Node) {
    this.synchronized {
      plan.put(request, Some(node))
    }
  }

  def nodeLost(request: RequestTemplate) {
    this.synchronized {
      plan.put(request, None)
    }
  }

  def requestRemoved(request: RequestTemplate) {
    this.synchronized {
      plan.put(request, None)
    }
  }

  def applicableRequest(resourceOffer: ResourceOffer): Option[RequestTemplate] = {
    this.synchronized {
      plan.collect {
        case (request, None) =>
          request
      }.find(rq => resourceOffer.matches(rq))
    }
  }
}

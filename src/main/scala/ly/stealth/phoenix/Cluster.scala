package ly.stealth.phoenix

import org.apache.mesos.Protos.Offer

import scala.beans.BeanProperty
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

  private def matchDoubles(requested: Option[Double], offered: Option[Double]) = {
    requested.forall(r => offered.exists(o => o >= r))
  }

  def matches(requestTemplate: RequestTemplate): Boolean = {
    matchDoubles(requestTemplate.cpus, cpus) &&
      matchDoubles(requestTemplate.mem, mem)
  }
}

case class RequestTemplate(id: Int,
                           cpus: Option[Double], mem: Option[Double],
                           configOverrides: Map[String, String])

object State {
  val Added = "added"
  val Stopped = "stopped"
  val Staging = "staging"
  val Running = "running"
}

case class TaskData(id: String, slaveId: String, executorId: String, attributes: Map[String, String],
                     cpus:Double, mem:Double)

case class Server(id: Int,
                  var requestTemplate: RequestTemplate,
                  var taskData: Option[TaskData] = None,
                  var state: String = State.Added)

class Cluster {
  @BeanProperty val servers = new mutable.ListBuffer[Server]()

  def getServerByTaskId(taskId: String): Option[Server] = {
    this.synchronized {
      servers.find {
        server => server.taskData.map(_.id).contains(taskId)
      }
    }
  }

  def getServer(id: Int): Option[Server] = {
    this.synchronized {
      servers.find {
        server => server.id == id
      }
    }
  }

  def addRequest(request: RequestTemplate): Unit = {
    this.synchronized {
      getServer(request.id) match {
        case Some(s) =>
          throw new IllegalArgumentException(s"Server with requested id is already exists ${request.id}")
        case None =>
          servers += new Server(request.id, request)
      }
    }
  }

  def deleteServer(id: Int): Unit = {
    this.synchronized {
      servers.indexWhere {
        server => server.id == id
      } match {
        case -1 =>
          throw new IllegalArgumentException(s"Node with $id doesn't exist")
        case index =>
          servers.remove(index)
      }
    }
  }

  def requestSatisfied(server: Server) {
    this.synchronized {
      servers.find {
        server => server.id == server.id
      }.foreach(_.state = State.Running)
    }
  }

  def serverLost(serverId: Int) {
    this.synchronized {
      servers.find {
        server => server.id == serverId
      }.foreach(_.state = State.Stopped)
    }
  }

  def applicableRequest(resourceOffer: ResourceOffer): Option[Server] = {
    this.synchronized {
      servers.filter(s => s.state == State.Stopped || s.state == State.Added).find {
        server => resourceOffer.matches(server.requestTemplate)
      }
    }
  }


}
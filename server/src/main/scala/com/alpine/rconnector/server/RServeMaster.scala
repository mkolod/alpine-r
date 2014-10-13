/*
 * This file is part of Alpine Data Labs' R Connector (henceforth " R Connector").
 * R Connector is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * R Connector is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with R Connector.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.alpine.rconnector.server

import java.util.{ Timer, TimerTask }

import akka.actor._
import akka.event.Logging
import com.alpine.rconnector.messages._
import com.alpine.rconnector.messages.RRequest
import com.typesafe.config.ConfigFactory
import org.rosuda.REngine.{ REXPMismatchException, REngineEvalException, REngineException }
import org.rosuda.REngine.Rserve.RserveException
import scala.collection.mutable
import scala.collection.mutable.{ HashMap, HashSet, Queue }
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.{ Escalate, Restart, Stop }

/**
 * This class does the routing of requests from clients to the RServeActor, which then
 * talks directly to R. It also supervises the RServeActors, restarting them in case of
 * connection failures, resumes them in case of R code evaluation failures (bugs in user code), etc.
 *
 * @author Marek Kolodziej
 * @since 6/1/2014
 */
class RServeMaster extends Actor {

  private implicit val log = Logging(context.system, this)

  logActorStart(this)

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 60, withinTimeRange = 1 minute) {

    case e @ (_: ActorInitializationException) => {
      logFailure(e)
      Stop
    }
    case e @ (_: Exception) => {
      logFailure(e)
      Restart
    }

    case e @ (_: Throwable) => Escalate
  }

  private val config = ConfigFactory.load().getConfig("rServeKernelApp")
  protected val numRoutees = config.getInt("akka.rServe.numActors")
  protected val timeoutMillis = config.getInt("akka.rServe.timeoutMillis")
  protected val frameSize: Long = {
    val str = config.getString("akka.remote.netty.tcp.maximum-frame-size")
    str.substring(0, str.length - 1).toLong
  }

  protected val jobQueue = new Queue[RequestRSession]()

  log.info(s"\n\nNumber of expected R workers = $numRoutees\n\n")
  log.info(s"\n\nMaximum R session duration (ms): $timeoutMillis\n\n")

  /* rServeRouter is a var because the RServeMaster may keep on running while
  the RServeActors may be shut down by the client by sending the RStop
  message to RServeMaster. The actors can then be restarted by having
  the client send the RStart message. */
  protected var rServeRouter: Option[Vector[ActorRef]] = createRServeRouter()

  /* Call to create the router, either upon RServeMaster start
     or to restart when the RStart message is received, after a prior
     receipt of the RStop message.
   */
  protected def createRServeRouter(): Option[Vector[ActorRef]] = {
    log.info(s"\nCreating R server router with $numRoutees actors")
    Some(Vector.fill(numRoutees)(context.actorOf(Props[RServeActorSupervisor])))
  }

  private def availableActors =
    rServeRouter.map(_.toSet &~ rWorkerSessionMap.values.toSet)

  /* Map to hold session UUID keys and corresponding ActorRef values.
     This allows the R session to be sticky and for the messages from the client
     to be redirected to the appropriate R worker, despite the load balancing.
     This is necessary for multiple R requests to be made by the client,
     e.g. for streaming data in chunks from Java to R, and other situations
     that require the Java-R connection be to be stateful beyond a single request.
   */
  private val rWorkerSessionMap = new HashMap[String, ActorRef]

  /*
   * This is a map of the session UUIDs to remote ActorRefs. This will allow us
   * to kill the sessions in case of a heartbeat failure.
   */
  private val remoteClientSessionMap = new HashMap[String, ActorRef]

  //  /* Find if there is a free actor and send its ActorRef, otherwise send None.
  //     This is necessary because there may be more Java requests to R workers
  //     than there are workers available.
  //   */
  //  private def resolveActor(uuid: String, remoteActorRef: ActorRef): Option[ActorRef] = {
  //
  //    if (rWorkerSessionMap.contains(uuid)) {
  //
  //      log.info(s"Found actor responsible for session $uuid - it is ${rWorkerSessionMap(uuid)}")
  //      Some(rWorkerSessionMap(uuid))
  //
  //    } else {
  //
  //      //   val availableActors = rServeRouter.map(_.toSet &~ rWorkerSessionMap.values.toSet)
  //
  //      log.info(s"\nNo bound actors for session $uuid found - picking available actor from $availableActors\n")
  //
  //      availableActors match {
  //
  //        case Some(set) if !set.isEmpty => {
  //
  //          val unusedActor = set.head
  //          log.info(s"\n\nFound unused actor for session $uuid - it is $unusedActor\n\n")
  //          rWorkerSessionMap += (uuid -> unusedActor)
  //          remoteClientSessionMap += (uuid -> remoteActorRef)
  //          log.info(s"\n\nAdding heartbeat for remote session $uuid\nfor actor $remoteActorRef\n\n")
  //          context.watch(remoteActorRef)
  //          setTimeoutTimerForUUID(uuid, timeoutMillis)
  //          Some(unusedActor)
  //        }
  //
  //        case _ => {
  //          log.info(s"No available actor found for session $uuid")
  //          None
  //        }
  //      }
  //    }
  //  }

  /*
   This needs to change the logic. If the session is in progress, permit operation.
   Otherwise, send the RActorIsNotAvailable message.
   */
  private def resolveActor(uuid: String, remoteActorRef: ActorRef): Option[ActorRef] = {

    def addToWorkerMap: Option[ActorRef] =
      if (!availableActors.isEmpty) {
        rWorkerSessionMap += (uuid -> availableActors.get.head)
        rWorkerSessionMap.get(uuid)
      } else None

    rWorkerSessionMap.get(uuid).orElse(addToWorkerMap).orElse(None)
  }

  /*
    TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    Check if this is still necessary
   */
  /*
   Time out in case the session hasn't been unbound by now
   (including if Alpine crashed but the Akka R server is running)
   */
//  private def setTimeoutTimerForUUID(uuid: String, timeoutMillis: Long): Unit = {
//    val tt = new TimerTask {
//      override def run(): Unit = {
//        rWorkerSessionMap.get(uuid).map(_ ! FinishRSession(uuid))
//        // wait for unbinding
//        Thread.sleep(5000)
//        rWorkerSessionMap.remove(uuid)
//      }
//    }
//    new Timer(uuid).schedule(tt, timeoutMillis)
//  }

  /*
    TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
   */
  private def dequeueNextSession(): Unit = {

    if (!jobQueue.isEmpty) {
      val session = jobQueue.dequeue()
      resolveActor(session.uuid, session.requester)
      session.requester ! RequestRSessionAck(session.uuid)
    }

  }

  /* The main message-processing method of the actor */
  def receive: Receive = {

    case x @ RequestRSession(uuid, remoteRef) => {
      jobQueue.enqueue(x)
      remoteClientSessionMap += (uuid -> remoteRef)
      // remote heartbeat / DeathWatch
      context.watch(remoteRef)
      log.info(s"Remote actor $remoteRef is requested session for UUID $uuid.")
      log.info("Response will be sent back when the session is available.")
    }

    case x @ Terminated(ref) => {
      /*
        TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
        1) If remote client was only waiting for a session to be available:
           - Remove from queue
           - Remove from remoteClientSessionMap

        2) If session was in progress:
           - Remove from remoteClientSessionMap
           - Terminate R session
        TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
       */
    }

    //      def currentRemoteRefs = remoteClientSessionMap.values.toList.sorted
    //      def currentRemoteUUIDs = remoteClientSessionMap.keySet.toList.sorted
    //      def currentRWorkerUUIDs = rWorkerSessionMap.keySet.toList.sorted
    //
    //   In case there are more, we'll have to get rid of the temp actors from outside of the Alpine actor system
    //   and create a permanent typed actor that can be accessed using method calls.
    //   Currently we have a distinction between the temp actors called outside of the actor system
    //   and the permanent forwarding actor.
    // */
    //      //      val sessionUuidsToTerminate = remoteClientSessionMap.filter(_._2 == ref).keySet.toList.sorted
    //      val sessionUuidsToTerminate = remoteClientSessionMap.keySet.toList.sorted
    //
    //      log.info(
    //        s"""Heartbeat to remote actor ($ref) has been severed.
    //
    //            Current remote actor refs:
    //            $currentRemoteRefs
    //
    //            Current session UUIDs associated with remote actors:
    //            $currentRemoteUUIDs
    //
    //            Current session UUIDs associated with existing R workers (should be the same as the above):
    //            $currentRWorkerUUIDs
    //
    //            Session UUIDs to terminate for the failed connection:
    //            $sessionUuidsToTerminate
    //          """.stripMargin)
    //
    //      //      sessionUuidsToTerminate.flatMap { id => rWorkerSessionMap.get(id).map {ref =>
    //      //        log.info(s"Sending message $id to finish R session")
    //      //        ref ! FinishRSession(id)
    //      //      }}
    //
    //      // finish R sessions
    //      for { id <- sessionUuidsToTerminate; ref <- rWorkerSessionMap.get(id) } {
    //        log.info(s"Sending Kill message to actor $ref to finish R session and restart the actor.")
    //        ref ! Kill
    //      }
    //
    //      // remove the sessions from the rWorkerSessionMap
    //      for { id <- sessionUuidsToTerminate } {
    //        log.info(s"Removing session id $id from rWorkerSessionMap")
    //        rWorkerSessionMap -= id
    //      }
    //
    //      log.info(
    //        s"""Removed the following sessions from R worker session map:
    //            $sessionUuidsToTerminate
    //
    //            Post-cleanup sessions associated with R workers:
    //            $currentRWorkerUUIDs
    //         """.stripMargin)
    //
    //      // remove the sessions from remoteClientSessionMap
    //      for { id <- sessionUuidsToTerminate } {
    //        log.info(s"Removing session id $id from remoteClientSessionMap")
    //        remoteClientSessionMap -= id
    //      }
    //
    //      log.info(
    //        s"""Removed the following sessions from remoteClientSessionMap:
    //             $sessionUuidsToTerminate
    //
    //            Pre-cleanup sessions associated with remote actors:
    //            $currentRemoteUUIDs
    //         """.stripMargin
    //      )
    //
    //      log.info(
    //        s"""Currently available R worker actors:
    //            $availableActors
    //         """.stripMargin)
    //    }

    case x @ RRequest(uuid, rScript, returnSet) => {

      log.info(s"\n\nRServeMaster: received RRequest request and routing it to RServe actor\n\n")

      resolveActor(uuid, sender) match {

        case Some(ref) => ref.tell(x, sender)
        case None => {
          log.info(s"\n\nRRequest: R actor is not available for sender $sender and session UUID $uuid\n\n")
          sender ! RActorIsNotAvailable
        }
      }
    }

    case x @ RAssign(uuid, dataFrames) => {

      log.info(s"\n\nRServeMaster: received RAssign request and routing it to RServe actor\n\n")

      resolveActor(uuid, sender) match {

        case Some(ref) => ref.tell(x, sender)
        case None => {
          log.info(s"\n\nRAssign: R actor is not available\n\n")
          sender ! RActorIsNotAvailable
        }
      }
    }

    /*
      TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
      Check if this behavior is still in use
      TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
     */
    /* Restart the router after it was previously stopped via RStop */
    case RStart => {

      log.info(s"\n\nMaster: Starting router\n\n")

      if (rServeRouter == None) {
        rServeRouter = createRServeRouter()
        log.info(s"\n\nRStart: created router\n\n")
      } else {
        log.info(s"\n\nR actor router was already started, ignoring request to start\n\n")
      }
      sender ! StartAck
    }

    /*
      TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
      Check if this behavior is still in use
      TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
     */
    /* Stop the router, shut down the R workers, and clear the R session map */
    case RStop => {

      // TODO: need to shutdown RServeMaster, and this should be handled by its newly created supervisor
      log.info(s"\n\nMaster: Stopping router\n\n")

      rServeRouter.foreach(_.foreach(_.tell(PoisonPill, self)))
      rServeRouter = None
      rWorkerSessionMap.clear()

      sender ! StopAck
    }

    /* Unbind an individual R session */
    case x @ FinishRSession(uuid) => {

      log.info(s"\n\nFinishing R session for client ID $uuid\n\n")
      log.info(s"\nAvailable actors before unbinding: $availableActors\n\n")

      rWorkerSessionMap.get(uuid).map(_.tell(x, sender))
      rWorkerSessionMap.remove(uuid)

      /*
        TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
        Do I need to remove it from remoteClientSessionMap?
       */
      log.info(s"\n\nR session $uuid unbound from R actor router\n")
      log.info(s"\nAvailable actors after unbinding: $availableActors\n\n")

      dequeueNextSession()

    }

    /* Get maximum number of R workers (to set the client's blocking queue size */
    case GetMaxRWorkerCount => {

      log.info(s"\n\nGetMaxRWorkerCount: ${numRoutees}\n\n")
      sender ! MaxRWorkerCount(numRoutees)
    }

    /* In case the client wants to know how many workers are free, e.g. for resource monitoring */
    case GetFreeRWorkerCount => {
      log.info(s"\n\nGetFreeRWorkerCount: ${numRoutees - rWorkerSessionMap.keySet.size}\n\n")
      sender ! FreeRWorkerCount(numRoutees - rWorkerSessionMap.keySet.size)
    }

    case RemoteFrameSizeRequest => {
      log.info(s"\n\nResponding with frame size: $frameSize")
      log.info(s"\n\nThe sender is $sender\n\n")
      sender ! RemoteFrameSizeResponse(frameSize)
    }
  }

}

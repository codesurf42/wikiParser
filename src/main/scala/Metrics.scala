import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.agent.Agent
import scala.collection.mutable

/**
 * Created by ab on 14.06.14.
 */

case class ExecTime(name:String, time:Long)
case class ShowTime()

class Metrics extends Actor {
  val counter = new mutable.HashMap[String, Long]() with mutable.SynchronizedMap[String, Long]
  val execTime = new mutable.HashMap[String, Long]() with mutable.SynchronizedMap[String, Long]

  def receive: Receive = {
    case ExecTime(name, time) =>
      if (!execTime.contains(name)) execTime.put(name, 0)
      if (!counter.contains(name)) counter.put(name, 0)

      execTime(name) += time
      counter(name) += 1

    case ShowTime() =>
      println("execTime:")
      execTime
        .toSeq
        .sortBy(-_._2)
        .foreach(
          kv =>
            println(kv._1, kv._2, counter(kv._1))
        )
    case _ =>
  }

/*
  val counter = Agent(mutable.HashMap[String, Int])

  override def receive: Receive = {
    case ExecTime(time, name) =>
      counter.send(update(_))
  }

  def update(kv: mutable.HashMap[String, Int]):mutable.HashMap[String, Int] = {
    kv
  }
*/
}

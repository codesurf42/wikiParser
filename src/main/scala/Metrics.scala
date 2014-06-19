import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.agent.Agent
import scala.collection.mutable

/**
 * Created by ab on 14.06.14.
 */

case class ExecTime(name:String, time:Long)
case class ShowTime()

object MetricsStorage {
  val counter = new mutable.HashMap[String, Long]() with mutable.SynchronizedMap[String, Long]
  val execTime = new mutable.HashMap[String, Long]() with mutable.SynchronizedMap[String, Long]
}

class Metrics extends Actor {
  val execTime = MetricsStorage.execTime
  val counter = MetricsStorage.counter

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
            println(f"${kv._1}%25s: ${(kv._2 / 1000).toInt}%12d, ${counter(kv._1)}%10d")
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

object TimeLib {
  def getTime:Long = {
    System.nanoTime()
  }
}

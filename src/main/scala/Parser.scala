import scala.concurrent.ExecutionContext.Implicits.global
import akka.agent.Agent
import akka.actor.Actor.Receive
import akka.actor._
import akka.event.Logging
import akka.routing.RoundRobinPool
import scala.io.Source
import scala.xml.pull._
import scala.xml.pull.EvComment
import scala.xml.pull.EvElemEnd
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvEntityRef
import scala.xml.pull.EvProcInstr
import scala.xml.pull.EvText

/**
 * Created by ab on 10.06.14.
 */
case class XmlFilename(name: String)
case class Article(title: String, text: String)
case class ArticleSummary(title: String, length: Int)

class FileReader extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = {
    case XmlFilename(name) =>
      readXmlFile(name)
      context.actorSelection("/user/longestArticle") ! "stats"
  }

  def readXmlFile(name: String) = {
    log.debug(s"Reading file $name")
    // parse xml content
    val xml = new XMLEventReader(Source.fromFile(name))
    loopXml(xml)

    def loopXml(xml: XMLEventReader) {
      var inPage = false
      var inPageText = false
      var inPageTitle = false
      var skipPage = false
      var pageText = ""
      var lastTitle = ""

      while (xml.hasNext) {
        log.debug("next: " + xml)
        xml.next match {
//          case EvElemStart(pre, label, attrs, scope) if label == "mediawiki" =>
//            println(s"!!! - $label")
          case EvElemStart(_, "page", _, _) =>
            inPage = true
          case EvElemStart(_, "title", _, _) if inPage =>
            inPageTitle = true
          case EvElemStart(_, "redirect", _, _) if inPage => // just skip them now
            skipPage = true

          case EvElemStart(_, label, _, _) if inPage && label == "text" =>
            inPageText = true
          case EvElemStart(pre, label, attrs, scope) if inPage =>
            log.debug(s"Elem of page: <$label>")
          case EvElemStart(pre, label, attrs, scope) =>
            log.debug("START: ", pre, label, attrs, scope)

          case EvElemEnd(_, "page") =>
            inPage = false
            skipPage = false
            lastTitle = "" // safer to clear

          case EvElemEnd(_, "title") =>
            inPageTitle = false
          case EvElemEnd(_, "text")=>
            log.debug(s"Got full article text [$lastTitle] - process it!")
            context.actorSelection("/user/article") ! Article(lastTitle, pageText)
            pageText = ""
            inPageText = false

          case EvElemEnd(pre, label) => log.debug("END: ",pre, label)

          case EvText(text) if inPageTitle =>
            lastTitle = text
          case EvText(text) if inPageText && !skipPage =>
            pageText += text

          case EvText(text) => log.debug("TEXT: " + text)

          case EvEntityRef(entity) if inPageText =>
            // TODO: add pageText entities to text!!! (how about entities from titles?)
            log.debug(s"Entity in text: ${entity}")
          case EvEntityRef(entity) => log.debug("ENTITY: " + entity)
          //        case POISON =>
          case EvProcInstr(target, text) => log.debug(s"PROCINSTR: $target, $text")
          case EvComment(text) => log.debug(s"EVCOMMENT: $text")
          case _ =>
        }
      }
    }

  }
}

class ArticleParser extends Actor {
  val log = Logging(context.system, this)

  def receive: Receive = {
    case art: Article => parseArticle(art)
    case _ =>
  }

  def parseArticle(art: Article) = {
    val limit = 50000
    log.debug(s"Parsing article: [${art.title}}] == " +
      art.text.substring(0,(
        if (art.text.length() > limit) limit
        else art.text.length()))
      + "..."
    )

    context.actorSelection("/user/longestArticle") ! ArticleSummary(art.title, art.text.length())
  }
}

class LongestArticle extends Actor {
  val log = Logging(context.system, this)
  var max = 0
  var count = 0

  override def receive: Receive = {
    case e: ArticleSummary =>
      log.debug(s"Got: ${e.title} [${e.length}]")
      count += 1
      Parser.agentCount.send(_ + 1)
      if (e.length > max) max = e.length
    case "stats" => println(s"LongestArt: $max, count: ${Parser.agentCount.get()}, $count")
  }
}

object Parser extends App {

  implicit val system = ActorSystem("parser")
  //  val log = Logger

  val reader = system.actorOf(Props[FileReader], "reader")
  val parser = system.actorOf(Props[ArticleParser], "article")
  val art = system.actorOf(Props[LongestArticle], "longestArticle")
  val agentCount = Agent(0)

  println("Sending flnm")
  val inbox = Inbox.create(system)
  inbox.send(reader, XmlFilename("/home/ab/data1_ab/tmp/wiki/enwiki_part1.xml"))

}

import akka.routing.RoundRobinRouter
import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import akka.agent.Agent
import akka.actor._
import akka.event.Logging
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

class XmlReader extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = {
    case XmlFilename(name) =>
      readXmlFile(name)
      context.actorSelection("/user/longestArticle") ! "stats"
    case _ =>
  }

  def readXmlFile(name: String) = {
    log.debug(s"Reading file $name")
    // parse xml content

    val t1 = new Date().getTime

    val xml = new XMLEventReader(Source.fromFile(name))
    parseXml(xml)

    println("Exec time: " + (new Date().getTime - t1))

  }

  def parseXml(xml: XMLEventReader) {
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
        case EvElemStart(_, "page", _, _)               => inPage = true
        case EvElemStart(_, "title", _, _) if inPage    => inPageTitle = true
        case EvElemStart(_, "redirect", _, _) if inPage => skipPage = true // just skip them now

        case EvElemStart(_, label, _, _) if inPage && label == "text" => inPageText = true
        case EvElemStart(pre, label, attrs, scope) if inPage =>
          log.debug(s"Elem of page: <$label>")
        case EvElemStart(pre, label, attrs, scope) => log.debug("START: ", pre, label, attrs, scope)

        case EvElemEnd(_, "page") =>
          inPage = false
          skipPage = false
          lastTitle = "" // safer to clear

        case EvElemEnd(_, "title")  => inPageTitle = false
        case EvElemEnd(_, "text")   =>
          log.debug(s"Got full article text [$lastTitle] - process it!")
          context.actorSelection("/user/article") ! Article(lastTitle, pageText)
          pageText = ""
          inPageText = false

        case EvElemEnd(pre, label)                    => log.debug("END: ",pre, label)
        case EvText(text) if inPageTitle              => lastTitle = text
        case EvText(text) if inPageText && !skipPage  => pageText += text
        case EvText(text)                             => log.debug("TEXT: " + text)

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

    if (false) {
    val ap = new ArticleParsingLib()
    val geoPos = ap.getGeo(art)
    val seePlaces = ap.getSeePlaces(art)
    } else {
      val geoPos = context.actorSelection("/user/geoParser") ! art
      val seePlaces = context.actorSelection("/user/seePlaces") ! art
    }
  }
}

class ArticleGeoParser extends Actor {
  val ap = new ArticleParsingLib()
  override def receive: Actor.Receive = {
    case e: Article => sender ! ap.getGeo(e)
    case _ =>
  }
}

class ArticleSeePlacesParser extends Actor {
  val ap = new ArticleParsingLib()
  override def receive: Actor.Receive = {
    case e: Article => sender ! ap.getSeePlaces(e)
    case _ =>
  }
}

class ArticleParsingLib {

  def getGeo(art: Article):Seq[String] = {
    val text = art.text
    val pos = text.indexOf("{{geo|")
    if (pos > 0) {
      val pos2 = text.indexOf("}}", pos + 5)
      val geoFields = text.substring(pos + 6, pos2).split('|')
      if (geoFields.size >= 2) {
        return geoFields
      }
    }
    Seq()
  }

  def getSeePlaces(art: Article):Int = {
    val pos = art.text.indexOf("\n==See==\n")
    pos
  }
}

class LongestArticle extends Actor {
  val log = Logging(context.system, this)
  var max = 0
  var count = 0 // naive implementation, this will break when a pool of thread > 1 / actor

  override def receive: Receive = {
    case e: ArticleSummary =>
      log.debug(s"Got: ${e.title} [${e.length}]")
      count += 1
      if (count % 1000 == 0) println(count)
      Parser.agentCount.send(_ + 1)
      if (e.title.length > Parser.agentMaxArtTitleLen.get()) {
        max = e.title.length
        Parser.agentMaxArtTitleLen.send(max)
        Parser.agentMaxArtTitle.send(e.title)
      }
    case "stats" =>
      println(s"LongestArt: $max (${Parser.agentMaxArtTitle.get()}), count: ${Parser.agentCount.get()}, $count")
  }
}

object Parser extends App {

  implicit val system = ActorSystem("parser")
  //  val log = Logger

  val reader = system.actorOf(Props[XmlReader], "reader")
  val parser = system.actorOf(Props[ArticleParser].withRouter(RoundRobinRouter(3)), "article")
  val art = system.actorOf(Props[LongestArticle].withRouter(RoundRobinRouter(2)), "longestArticle")
  val geo = system.actorOf(Props[ArticleGeoParser].withRouter(RoundRobinRouter(2)), "geoParser")
  val seePl = system.actorOf(Props[ArticleSeePlacesParser], "seePlaces")

  val agentCount = Agent(0)
  val agentMaxArtTitle = Agent("")
  val agentMaxArtTitleLen = Agent(0)

  println("Sending flnm")
  val inbox = Inbox.create(system)
  val file = if (false) "/home/ab/data1_ab/tmp/wiki/enwiki_part1.xml"
    else "/home/ab/data1_ab/tmp/wiki/enwikivoyage-20140520-pages-articles.xml"
  inbox.send(reader, XmlFilename(file))

}

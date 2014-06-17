/**
 * Created by ab on 17.06.14.
 */
class WikiParsing {

  def getGeo(art: Article):Seq[String] = {
    val t1 = TimeLib.getTime

    val text = art.text
    val pos = text.indexOf("{{geo|")
    if (pos > 0) {
      val pos2 = text.indexOf("}}", pos + 5)
      val geoFields = text.substring(pos + 6, pos2).split('|')
      if (geoFields.size >= 2) {
        Parser.met ! ExecTime("getGeo-1", TimeLib.getTime - t1)
        return geoFields
      }
    }
    Parser.met ! ExecTime("getGeo-2", TimeLib.getTime - t1)
    Seq()
  }

  def getSeePlaces(art: Article):Int = {
    val t1 = TimeLib.getTime

    val size = getSection(art.text, "==See==").size + getSection(art.text, "==Do==").size

    Parser.met ! ExecTime("seePlaces", TimeLib.getTime - t1)
    Parser.met ! ExecTime("seePlacesLength", size)

    size
  }

  def getSection(wikiText: String, section: String):String = {
    val reHeader = """(={2,3})([^\n]{1,50})\1""".r
    val reHeader(prefix, name) = section

    //    println(s"prefix: $prefix, name=$name")

    val reSection = ("""(?s)\n""" + prefix + name + prefix + "\\n" +
      """(.*?)""" +
      """(?:""" +
      """(?:\n""" + prefix + """[^\n]{1,50}""" + prefix + """\n)""" +
      """|\z)""").r

    Thread.sleep(1)

    reSection.findFirstIn(wikiText) match {
      case Some(reSection(content)) => content
      case None                     => ""
    }
  }

}

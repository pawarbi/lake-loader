package utils

import scala.io.Source
import scala.util.{Try, Using}

object FileUtils {

  def loadDoubles(filePath: String): List[Double] = {
    Using.resource(Source.fromFile(filePath)) { source =>
      source.getLines().flatMap(line => Try(line.toDouble).toOption).toList
    }
  }

  def loadLongs(filePath: String): List[Long] = {
    Using.resource(Source.fromFile(filePath)) { source =>
      source.getLines().flatMap(line => Try(line.toLong).toOption).toList
    }
  }
}

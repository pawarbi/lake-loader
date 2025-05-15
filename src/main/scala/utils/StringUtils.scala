package utils

object StringUtils {
  def generateRandomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "!@#$%^&*()-_=+[]{};:,.<>/?".toSeq
    val r = new scala.util.Random()
    (1 to length).map(_ => chars(r.nextInt(chars.length))).mkString
  }

}

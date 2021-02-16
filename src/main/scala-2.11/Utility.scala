import java.io.File
import java.nio.file.{Files, Paths}
import org.apache.commons.io.FileUtils
import util.Try

object Utility {
  def reName(fromFolder: String, to: String) = {
    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile)
        .filter(_.getName.startsWith("part-"))
        .map(_.getPath).toList
    }
    val list=getListOfFiles(fromFolder)
    Try(new File(list(0)).renameTo(new File(to))).getOrElse(false)
  }
  // example: "Unibench/Graph_SocialNetwork/PersonHasInterests/person_has_interests.csv"

  def Copy(fromFolder: String, to: String) = {
    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile)
        .filter(_.getName.startsWith("tag_"))
        .map(_.getPath).toList
    }
    Files.createDirectories(Paths.get("Unibench/Graph_SocialNetwork/Tag"))
    val list=getListOfFiles(fromFolder)
    FileUtils.copyFile(new File(list(0)), new File(to));
    //Try(new File(list(0)).renameTo(new File(to))).getOrElse(false)
  }

}
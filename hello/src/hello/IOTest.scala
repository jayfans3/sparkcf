package hello
import reflect.io.Path
import scala.collection.immutable.SortedMap
/**
 * Created by ocean on 6/30/15.
 */
object IOTest {

def  main (args: Array[String]) {

      val fnm = "/home/ocean/app/test.txt"
      def wrt(p :  Iterator[Path]) {
        import java.io._
        val writer = new PrintWriter(new File(fnm),"UTF-8")
        var i = 0
        p.foreach{  f =>
          i += 1
          writer.write(i + "," + f +"\n")
        }
        writer.close
      }
      def rd = {
        import scala.io.Source
        var sm = SortedMap[String,Int]()
        val s = Source.fromFile(fnm,"UTF-8").getLines.foreach{s =>
          val ss = s.split(",")
          sm += ss(1) -> ss(0).toInt
        }
        sm
      }
      def lstFile = {
        val path = "/home/ocean/app"
        Path(path).walkFilter (p =>  p.isDirectory || """a*.txt""".r.findFirstIn(p.name).isDefined)
      }

      def main(args: Array[String]) {
        wrt(lstFile)
        rd.foreach(f => println(f))
      }
    }
}

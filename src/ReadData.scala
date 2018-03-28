package simpleskyline

import java.io.File

import scala.io.Source

/**
  * Created by user on 2017/3/1.
  */
object ReadData   extends App{
  //val file = new File("F:\\intellij_Test_File\\testData\\300_150_100.txt")
  val source=Source.fromFile("F:\\intellij_Test_File\\testData\\300_150_100.txt")
 // println(source.mkString)
   val iterator=source.getLines()
  for (l<- iterator)
    {
      println(l)
    }




}

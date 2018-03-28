package simpleskyline

import java.io.{File, PrintWriter}

/**
  * Created by user on 2017/6/7.
  */
object QueryData extends App{
  val RANGE=10000
  val queryNum=3
  val keyword_MaxNum=20
  val file = new File("F:\\intellij_Test_File\\testData\\Query.txt")
  val pw: PrintWriter = new PrintWriter(file)
  var i = 0
  val random = scala.util.Random
  while (i < queryNum) {
    val valueX = random.nextInt(RANGE)
    val valueY = random.nextInt(RANGE)
    val chars = "abcdefghijklmnopqrstuvwxyz"
    var keywordNum=random.nextInt(keyword_MaxNum-1)+1//the num of  one data point keyword
    var keywords=""
    while(keywordNum>0)//get the keywords of the data point
    {
      val j = (Math.random() * 26).toInt
      val keyword = chars.charAt(j)
      keywords+=" "+keyword
      keywordNum-=1
    }
    pw.write(valueX + " " + valueY + "," + keywords+"\n" )
    i += 1
  }
  print("query data successful")
  pw.close()

}

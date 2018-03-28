package simpleskyline

import java.io.{File, PrintWriter}


/**
  * Created by user on 2017/3/1.
  */
object CreatData extends App{
  val RANGE=10000//the range of data spatial
  val keyword_MaxNum=10 //the maxnum of the keywords for data object
  val file = new File("F:\\intellij_Test_File\\testData\\300_150_100.txt")
  val pw: PrintWriter = new PrintWriter(file)
  var i = 0//control the data number;
  val random = scala.util.Random
  while (i < 20) {
    val valueX = random.nextInt(RANGE)
    val valueY = random.nextInt(RANGE)
    val chars = "abcdefghijklmnopqrstuvwxyz"
    var keywordNum=random.nextInt(keyword_MaxNum-1)+1//the num of  one data point keyword
    var keywords=""
    while(keywordNum>0)//get the keywords of the data point
    {
      val j = (Math.random() * 26).toInt
      val keyword = chars.charAt(j)
      if(keywords.contains(keyword)!=true)//not contains the same keywords
      {
        keywords+=" "+keyword
        keywordNum-=1
      }

    }
    pw.write(valueX + " " + valueY + "," + keywords+"\n" )

//    pw.write(i+","+valueX + " " + valueY + "," + keywords+"\n" )
    i += 1
  }
  print("successful")
  pw.close()
}

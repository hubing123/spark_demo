package simpleskyline

import java.text.DecimalFormat

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD


/**
  * Created by user on 2017/6/7.
  */
object TF_IDF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)
    //    val documents:RDD[Seq[String]]=sc.textFile("F:\\intellij_Test_File\\testData\\tfidf.txt").map(_.split(" ").toSeq)
    val file = sc.textFile("F:\\intellij_Test_File\\testData\\300_150_100.txt")
    println("kkkkkkkkkkkk")
    val documents = file.map(line => {//documents: RDD[Seq[String]]
      val s = line.split(",")
      val text =s(s.size-1).split(" ").drop(1)
      text
    }.toSeq)//.foreach(println)
    val dataLocation: RDD[Seq[String]] = file.map(line => {
      val s = line.split(",")
      val text = s(0).split(" ").drop(1)
      text
    }.toList)//.toSeq
    val hashingTF = new HashingTF()

    val mapWords = documents.flatMap(x => x).map(w => (hashingTF.indexOf(w), w)).collect.toMap
    val tf: RDD[Vector] = hashingTF.transform(documents)
    val bcWords = tf.context.broadcast(mapWords)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val r = tfidf.map {
      case SparseVector(size, indices, values) =>
        val words = indices.map(index => bcWords.value.getOrElse(index, "null"))
        words.zip(values).sortBy(-_._2).toList//.toSeq
    }.foreach(println)
//    val result = dataLocation.zip(r).map(line=>(line._1,line._2)).foreach(println)//per line represents the object's location(x,y) and the revelence of per keywords
    /*val result = dataLocation.zip(r).map(line=>(line._1,line._2))//per line represents the object's location(x,y) and the revelence of per keywords

    val q1="123 234,t w h k"
    val spatialDis=result.map(line=>{
      val datalocation=line._1.mkString
      val spatial=distiance(datalocation,q1.split(",")(0))

    })*/
    println("success output")
    tfidf.foreach(x => println(x))
    println("success")
  }
  def distiance(queryData: String,datapoint:String):Double={//get the distances of querypoint to datapoint
  val dimNum=datapoint.split(" ").length
    val obj=datapoint.split(" ")
    val query=queryData.split(" ")
    var sum=0.0
    for(dim<- 0 to dimNum-1)
    {  sum+=Math.pow((obj(dim).toInt-query(dim).toInt),2)
    }
    val distance=DecimalFormat(Math.sqrt(sum))
    distance
  }
  def DecimalFormat(value:Double):Double={
    val df=new DecimalFormat(".##");
    df.format(value).toDouble
  }
 /* def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)
    val documents:RDD[Seq[String]]=sc.textFile("F:\\intellij_Test_File\\testData\\tfidf.txt").map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf:RDD[Vector]=hashingTF.transform(documents)
    tf.cache()
    println("tf")
    val idf =new IDF().fit(tf)
    val tfidf:RDD[Vector]=idf.transform(tf)
    println("success")
    println("tfidf: ")
    tfidf.foreach(x => println(x))


  }
 def main(args: Array[String]) {
   val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
   val sc = new SparkContext(conf)
   val file=sc.textFile("F:\\intellij_Test_File\\testData\\tfidf.txt")
   val documents:RDD[Seq[String]]=file.map(_.split(" ").toSeq)
//   val documents:RDD[Seq[String]]=sc.textFile("F:\\intellij_Test_File\\testData\\tfidf.txt").map(_.split(" ").toSeq)
   val hashingTF = new HashingTF()
   val mapWords=documents.flatMap(x=>x).map(w=>(hashingTF.indexOf(w),w)).collect.toMap
   val tf:RDD[Vector]=hashingTF.transform(documents)
   val bcWords=tf.context.broadcast(mapWords)
   tf.cache()
   val idf =new IDF().fit(tf)
   val tfidf:RDD[Vector]=idf.transform(tf)

//   val result=tfidf.map{
//     case SparseVector(size,indices,values)=>
//       val words=indices.map(index=>bcWords.value.getOrElse(index,"null"))
//       words.zip(values).sortBy(-_._2).toSeq
//   }.foreach(println)

   println("success output")
//   tfidf.foreach(x => println(x))

   println("success")
 }*/

}
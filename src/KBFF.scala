package simpleskyline

import org.apache.spark.mllib.clustering.{KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by user on 2017/11/20.
  */
object KBFF extends Serializable{
  def main(args: Array[String]): Unit = {
    val partitionGridsNumbers = 16
    val ParNumber = 16 //args(4).toInt//64
    val dataSpace = Array(0.0, 0.0, 10000.0, 10000.0)
    val Dim = 2

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val objFile = sc.textFile("F:\\intellij_Test_File\\testData\\300_150_100.txt")


    val queryFile = sc.textFile("F:\\intellij_Test_File\\testData\\Query.txt")
    val KEYSET= "abcdefghijklmnopqrstuvwxyz"

   val obj_hot= objFile.map(x=>{
      val obj_coor = x.split(",")(0)
      val obj_key = x.split(",")(1).toSet
      val onehot=for(item <- KEYSET)  yield (if(obj_key.contains(item)) "1 " else "0 ").toList.foldLeft("")((str,i)=>(str+i)).trim
//        (obj_coor,onehot)
        (onehot,obj_coor)
    })
println(obj_hot.first())
    val parsedData= obj_hot.map(s => Vectors.dense(s._2.split(' ').map(_.toDouble))).cache()
    val numClusters =10
    val numIterations = 20
    val clusters = KMeans.train(parsedData,numClusters,numIterations)

    println("Cluster Number:" + clusters.clusterCenters.length)

    val WSSSE=clusters.computeCost(parsedData)
    println(WSSSE)

    parsedData.foreach(i =>{println(clusters.predict(i)+"::"+i)})

    val cls = parsedData.map(iter =>{
      val clsID = clusters.predict(iter)
      (clsID,iter)
    }).groupByKey().foreach(println)//(3,CompactBuffer([0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,1.0,0.0,1.0], [0.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0]))
  }
  /*
  def get_keywords(vector:String):List[String]={//vector: Vector
  val str = vector.toString
    var list:List[String] = Nil
        for(i<-0 until  str.length){
      if(str.charAt(i)=="1"){
        val ch = (i+97).toChar
        list.::(ch.toString)
      }
    }
list
  }
  */
}

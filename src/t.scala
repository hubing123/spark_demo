package simpleskyline

import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by user on 2017/11/22.
  */
object t extends App{
  val M = 10
  val threshold = 0.3



  val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val objFile = sc.textFile("F:\\intellij_Test_File\\testData\\Rtree_Data.txt")

  val KEYSET= "abcdefghijklmnopqrstuvwxyz"
  val obj_hot= objFile.map(x=>{
    val arr=x.split(",")
    val obj_coor = arr(1)
    val obj_key = arr(2).toSet
    val hot = for(item <- KEYSET)  yield (if(obj_key.contains(item)) "1 " else "0 ")
    val onehot = hot.toList.foldLeft("")((str, i)=>(str+i)).trim
        (obj_coor,onehot)
  })
println(obj_hot.count())

  val parsedData= obj_hot.map(s => Vectors.dense(s._2.split(' ').map(_.toDouble))).cache()
  val numClusters =6//math.sqrt(parsedData.count()).toInt
  val numIterations = 500
  val clusters = KMeans.train(parsedData,numClusters,numIterations)
  val WSSSE=clusters.computeCost(parsedData)
  println(WSSSE)
  println("Cluster centres:")
  for(c <- clusters.clusterCenters) {
    println("  " + c.toString)
  }


  parsedData.foreach(i =>{println(clusters.predict(i)+"::"+i)})

println("groupbykey")
  val cls1 = parsedData.map(iter =>{
    val clsID = clusters.predict(iter)
    (clsID,iter)
  }).groupByKey()
  cls1.foreach(println)

println("topm")
  val clsSum=cls1.map(x=>{
    val id = x._1
    val Z = breeze.linalg.DenseVector.zeros[Double](26)
    val objset:breeze.linalg.DenseVector[Double]= x._2.map(y=>new breeze.linalg.DenseVector(y.toArray)).foldLeft(Z)(_ + _)
//    val kEYSETarr = KEYSET.toCharArray
//    val arr = {for(i<- 0 until objset.length if (objset(i)!=0.0)) yield (objset(i), KEYSET(i))}.sortBy(_._1).drop(M).map(_._2)
//    (id,arr)
    (id,objset)
     }).collect()
  clsSum.foreach(println)



  val kEYSETarr = KEYSET.toCharArray
  val clstopM:Seq[(Int,Array[Char])] = clsSum.map(y=>{
    val arr:Array[Char] = {
      for(i<- 0 until y._2.length  ) yield (y._2(i), KEYSET(i))
    }.sortBy(_._1).drop(26-M).map(_._2).toArray
    (y._1,arr)
  }).toSeq
  //clstopM.foreach(y=>println(y._1+":"+y._2.length))
  clstopM.foreach(y=>println(y._1+":"+y._2.toBuffer))

  val thresoldFilterHot =clstopM.map(x=>{
    val set = x._2.toSet
    val hot = for(item <- KEYSET)  yield (if(set.contains(item)) "1 " else "0 ")
    val onehot = hot.toList.foldLeft("")((str, i)=>(str+i)).trim
    (x._1,onehot.split(" ").map(_.toDouble).toVector)
  })
  thresoldFilterHot.foreach(println)

  val queryFile = sc.textFile("F:\\intellij_Test_File\\testData\\Rtree_Query.txt")
//  val queryHot = queryFile.map(x=>{
//    val arr=x.split(",")
//    val obj_coor = arr(1)
//    val obj_key = arr(2).toSet
//    val hot = for(item <- KEYSET)  yield (if(obj_key.contains(item)) "1 " else "0 ")
//    val onehot = hot.toList.foldLeft("")((str, i)=>(str+i)).trim
//    (obj_coor,onehot.split(" ").map(_.toDouble).toVector)
//  })
//  queryHot.foreach(y=>println(y._1+":"+y._2))





  //.foreach(println)
//    val ss=topm.map(y=>{
//        val kEYSETarr = KEYSET.toCharArray
//        val arr = {for(i<- 0 until y._2.length if (y._2(i)!=0.0)) yield (y._2(i), KEYSET(i))}.sortBy(_._1).drop(M).map(_._2)
//        (y._1,arr)
//  }).foreach(println)


/*
*
val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) => //vector可以理解为该点的坐标向量
    val pred = movieClusterModel.predict(vector)//pred为预测出的该点所属的聚点
    val clusterCentre = movieClusterModel.clusterCenters(pred)//clusterCentre为该pred聚点的坐标向量
    val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))//求两坐标的距离
    (id, title, genres.mkString(" "), pred, dist)
}
* */
/*
  parsedData.foreach(i =>{println(clusters.predict(i)+"::"+i)})
  val cls = parsedData.map(iter =>{
    val clsID = clusters.predict(iter)
  (clsID,iter)
  }).zipWithIndex().sortBy(_._2)
  println("jhgjhgjhgjhgjhgjhgjhgjhg"+cls.count())
  println("jhgjhgjhgjhgjhgjhgjhgjhg"+obj_hot.count())
  val obj = cls.zip(obj_hot).map(iter=>{
  val clsID = iter._1._1._1
  val objID=iter._1._2
  val obj_coor = iter._2._1
  val obj_onehot=iter._2._2
  (clsID,objID,obj_coor,obj_onehot)
}).foreach(println)

*/

//  def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum

//  val parsedData2:Array[Vector[Double]] = obj_hot.map(line=>{
//    val parts = line._2.split(" ").map(_.toDouble)
//    var vector = Vector[Double]()
//    for(i <- 0 to 26-1)
//      vector++=Vector(parts(i))
//    vector
//  }).toArray()
//  def junzhi(Data:Array[Vector[Double]]):Vector= {
//  for (i <- 0 to Data.length){
//
//  }


}

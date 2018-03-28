package simpleskyline

import java.text.DecimalFormat

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by user on 2017/6/7.
  */
object simpleSkyline extends Serializable{
  def main(args: Array[String]) {

    val partitionGridsNumbers = 16 //args(3).toInt//16
    val ParNumber = 16 //args(4).toInt//64
    val dataSpace = Array(0.0, 0.0, 10000.0, 10000.0)
    //val Dim = 2

//    val  start = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val objFile = sc.textFile("F:\\intellij_Test_File\\testData\\300_150_100.txt")
    val objKeywords: RDD[Seq[String]] = objFile.map(line => {
      val obj = line.split(",")
      val keywords = obj(1).split(" ").drop(1)
      keywords
    }.toSeq)
    val objCoordinate: RDD[Seq[String]] = objFile.map(line => {
      val obj = line.split(",")
      val coordinate = obj(0).split(" ")
      coordinate
    }.toList) //.toSeq
 println(objCoordinate.count())
    val hashingTF = new HashingTF()

    val mapWords = objKeywords.flatMap(x => x).map(w => (hashingTF.indexOf(w), w)).collect.toMap
    val tf: RDD[Vector] = hashingTF.transform(objKeywords)
    val bcWords = tf.context.broadcast(mapWords)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val r = tfidf.map {
      case SparseVector(size, indices, values) =>
        val words = indices.map(index => bcWords.value.getOrElse(index, "null"))
        words.zip(values).sortBy(-_._2).toList //.toSeq
    }

    val keywordsReve = objCoordinate.zip(r).map(line => (line._1, line._2))

    println("文本相关性")
    keywordsReve.foreach(println)


    val queryFile = sc.textFile("F:\\intellij_Test_File\\testData\\Query.txt")
    val querySet: Set[String] = Set()//declare a null set
    val queryDescribe = queryFile.map(line => {
      querySet + line.mkString
    }).collect()//the result is Set(6795,5235,u,z)

   val Dim=queryDescribe.length


    var resultQuery: RDD[(Int, (Double, Double))] = spatialTextReve(keywordsReve, queryDescribe(0).mkString)//求出与第一个查询点之间距离和文本相关性，将文本相关性为0.0的直接过滤
    if (queryDescribe.length > 1) {//求出与所有查询点之间距离和文本相关性，将文本相关性为0.0的直接过滤,且根据数据对象编号求并集
      for (queryIndex <- 1 until queryDescribe.length) {
        val currentQuery = spatialTextReve(keywordsReve, queryDescribe(queryIndex).mkString)
        resultQuery = resultQuery.union(currentQuery)
      }
    }
    val obj_query_Result = resultQuery.groupByKey().filter(line => line._2.size == queryDescribe.length)//将同时包含3个查询点的数据对象留下
    obj_query_Result.foreach(println) //(8,CompactBuffer((7953.51,1.7), (3663.85,1.7), (3368.65,1.7)))其中8是空间对象的编号，后边分别是到每个查询点的距离和文本相关性
    println("data processing end,success")

//    val getFirstRightUpPoint: Array[(Double)] = ZOrder.getZOrderRightUpPoint(Dim, 0, dataSpace, partitionGridsNumbers)

    val dataObject: Array[(Int, (Int, Array[Double]))] = obj_query_Result.map(x => {
      //(网格编号，（下标，Array（dim1，dim2，……，dimn））)//:Iterator[(Int, (Int, Array[Double]))]
      val corValue = x._2.map(y => y._1 * 0.5 + (1/y._2) * 0.5).toArray
      val ZOrderNum = ZOrder.getZOrder(Dim, corValue, dataSpace, partitionGridsNumbers) //每个点映射到的网格编码（Int）
      (ZOrderNum, (x._1, corValue))
    }).collect().toIterator.toArray

        dataObject.foreach(x=>println(x._1,x._2._1,x._2._2.toList))  //这俩哪个都可以

    val ZOrderNum_Min = dataObject.map(_._1).toSet.min//get the min ZOrderNum contains objects
    val getFirstRightUpPoint: Array[(Double)] = ZOrder.getZOrderRightUpPoint(Dim, ZOrderNum_Min, dataSpace, partitionGridsNumbers)//get the 右上角的点

    dataObject.filter(x => {
      val current_leftLowPoint = ZOrder.getZOrderLeftLowPoint(Dim, x._1, dataSpace, partitionGridsNumbers)
      P1_NotDom_P2(getFirstRightUpPoint, current_leftLowPoint)
    }).toIterator.toArray

    val skyline: Array[(Int, Array[(Double)])] = sc.parallelize(dataObject,ParNumber).groupByKey().mapPartitions(iter=>iter.flatMap(y=>spatialSkyline(Dim,y._2.toArray))).collectAsMap().toArray
    spatialSkyline(Dim,skyline).map(obj=>obj._1).toList.foreach(println)

//    val  end = System.currentTimeMillis()
//    println(start-end)
    sc.stop()

  }

  def spatialSkyline(Dim: Int, dataObject: Array[(Int, Array[(Double)])]): Array[(Int, Array[(Double)])] = {
    //空间Skyline计算，得到Skyline集合
    val Dominator = DominatorSetCompute(Dim, dataObject)
    val arr: Array[Array[(Double)]] = dataObject.map(x => x._2)
    val Skyline: Array[Array[(Double)]] = dominateCompute(Dim, Dominator, arr)

    val SkylineID: Array[Int] = Skyline.map(x => findID(x, dataObject))
    val localSkyline: Array[(Int, Array[(Double)])] = SkylineID.zip(Skyline)
    localSkyline
  }

  def DominatorSetCompute(Dim: Int, dataObject: Array[(Int, Array[(Double)])]): Array[Array[(Double)]] = {
    //得到三个支配点，得到支配集合
    var Dominate: List[Array[(Double)]] = Nil
    val arr: Array[Array[(Double)]] = dataObject.map(x => x._2)
    var dimSum = 0.0
    for (i <- 0 until Dim) //the type of x is (Int, Array[(Double)]),x._1 is the index of this object,it's type is Int,and x._2 is the Array[(Double)]
    {
      val sortedDim: Array[Array[(Double)]] = arr.sortBy(y => {
        dimSum += y(i)
        y(i)
      })

      val so: Array[(Double)] = sortedDim(0)
      Dominate = so :: Dominate
    }
    val sortedDimSum: Array[(Double)] = arr.sortBy(y => dimSum).take(1).head
    Dominate = sortedDimSum :: Dominate
    Dominate.distinct.toArray
  }

  def dominateCompute(Dim: Int, dominator: Array[Array[(Double)]], dataObject: Array[Array[(Double)]]): Array[Array[(Double)]] = {
    //根据支配集，删除被支配的点，得到Skyline集合
    var SkylineList: List[Array[(Double)]] = dominator.toList
    for (obj <- dataObject) //obj is the Object (Int, Array[(Double)])
    {
      if (!P_Dominated_S(Dim, obj, SkylineList.toArray)) //如果当前对象 不能  被支配集合中的点所支配，那么判断该点能否支配候选集中的点，如果支配就将候选集中的点删除，将该点加入到候选集中，
      {
        SkylineList = obj :: Update_Dominate(Dim, obj, SkylineList.toArray).toList
      }
    }
    val Skyline = SkylineList.distinct.toArray
    Skyline
  }

  def findID(Point: Array[(Double)], dataSet: Array[(Int, Array[(Double)])]): Int = {
    val s = Point.foldLeft("")(_ + " " + _)
    val d = dataSet.map(x => {
      (x._1, x._2.foldLeft("")(_ + " " + _))
    })
    var id = -1
    var flag = true
    var index = 0
    while (index < d.length && flag) {
      val iter: (Int, String) = d(index)
      if (iter._2 == s) {
        id = iter._1
        flag = false
      }
      index = index + 1
    }
    id
  }


  def Update_Dominate(Dim: Int, Point: Array[(Double)], Set: Array[Array[(Double)]]): Array[Array[(Double)]] = {
    //判断一个点能否支配一个集合中的点，如果可以支配 在集合中删除该点
    Set.filter(!P1_Dominated_P2(Dim, Point, _))
  }

  def P_Dominated_S(Dim: Int, Point: Array[(Double)], Dom: Array[Array[(Double)]]): Boolean = {
    //判断一个点能否能被集合中的点支配，如果能被集合中的点支配，返回true
    var flag = false
    var i = 0
    while (i < Dom.length && !flag) {
      flag = flag | P1_Dominated_P2(Dim, Dom(i), Point)
      i += 1
    }
    flag
  }


  def P1_Dominated_P2(Dim: Int, Arr1: Array[(Double)], Arr2: Array[(Double)]): Boolean = {
    //two point,如果Arr1支配Arr2，则返回true，不能支配则返回false

    var flag = true
    var dim = 0
    var lessFlag = false
    while (dim < Dim && flag) {
      var currentFlag = false
      if (Arr1(dim) < Arr2(dim)) {
        currentFlag = true
        lessFlag = true
      }
      else if (Arr1(dim) == Arr2(dim)) {
        currentFlag = true
      }
      else
        currentFlag = false

      flag = flag & currentFlag
      dim += 1
    }
    flag & lessFlag
  }

  def P1_NotDom_P2(arr: Array[(Double)], brr: Array[(Double)]): Boolean = {
    //判断网格的支配关系（左下以及右上），如果能支配，则需要被删除，返回 false
    val length = arr.length
    var flag = 1
    for (i <- 0 until length) {
      flag = flag & (if (brr(i) >= arr(i)) 1 else 0)
    }
    if (flag == 1) false else true
  }

  def spatialTextReve(result: RDD[(Seq[String], List[(String, Double)])], q1: String): RDD[(Int, (Double, Double))] = {
    val spatialTextReve = result.map(line => {
      var dataLocation = ""
      for (i <- 0 until line._1.size) {
        dataLocation += line._1(i) + " "
      }
      val spatialDis = distance(dataLocation.trim, q1.split(",")(0).mkString)
      val keywords = line._2 //得到单词和单词的相关性，是一个List集合，如List((x,1.0116009116784799), (z,1.0116009116784799))
      val textRelevance = keywords.map {
          case (keyword, releValue) => if (q1.split(",")(1).contains(keyword)) DecimalFormat(releValue) else 0.0
        }.foldLeft(0.0)((sum, i) => sum + i)
      (spatialDis, textRelevance) //return the (distance,textRelevance) of pi to each datapoint
    }).zipWithIndex().filter(_._1._2 != 0.0).map {
      case (r, index) => (index.toInt, r)
    }
    spatialTextReve
  }


  def distance(datapoint: String, queryData: String): Double = {
    //get the distances of query point to data point
    val dimNum = queryData.split(" ").length
    val obj = datapoint.split(" ")
    val query = queryData.split(" ")
    var sum = 0.0
    for (dim <- 0 until dimNum) {
      sum += Math.pow(obj(dim).toDouble - query(dim).toDouble, 2)
    }
    val distance = DecimalFormat(Math.sqrt(sum))
    distance
  }

  def DecimalFormat(value: Double): Double = {
    val df = new DecimalFormat(".##")
    df.format(value).toDouble
  }
}

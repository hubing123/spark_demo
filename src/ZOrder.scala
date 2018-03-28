package simpleskyline

/**
  * Created by user on 2017/6/15.
  */
class ZOrder(val Dim: Int, val dataSpace: Array[(Double)], val slice: Int) extends Serializable {

}

object ZOrder {
  /**
    * Dim: the Dim of object
    * * dataSpace: the rectangle contains all space
    * slice: the grid number of dataSpace
    */
  def apply(Dim: Int, dataSpace: Array[(Double)], slice: Int): ZOrder = {
    new ZOrder(Dim, dataSpace, slice)
  }


  /**
    * Dim: the Dim of object
    * point: a data object
    * dataSpace: the Space contains all objects
    * gridsNum: the grid number of dataSpace
    */
  def getZOrder(Dim: Int, point: Array[(Double)], dataSpace: Array[(Double)], gridsNum: Int): Int = {
    //给一个点得出该点所在的网格单元的编号
    //得到该点的网格位置
    val zorderLength = (gridsNum - 1).toBinaryString.length / Dim
    val numSlice = Math.pow(Dim, zorderLength).toInt

    val allDimMinValue = dataSpace.take(Dim)
    val allDimMaxValue = dataSpace.takeRight(Dim)
    val gridLength = allDimMinValue.zip(allDimMaxValue).map(x => {
      val gridLength = (x._2 + 0.1 - x._1) / numSlice
      gridLength
    }) //得到每一维上应该划分的分片大小
    val st = point.zip(gridLength).map(x => {
        val i = (x._1 / x._2).toInt
        getBinary(i, zorderLength)
      })
    var currentStr = st(0).zip(st(1)).map(x => x._1 + "" + x._2)
    for (dim <- 2 to st.size - 1 if dim < Dim) {
      currentStr = currentStr.zip(st(dim)).map(x => x._1 + "" + x._2)
    }
    val str = currentStr.foldLeft("")(_ + _)
    Integer.parseInt(str, 2)
  }


  /**
    * Dim: the Dim of object
    * ZOrderValue: the ZOrder Value of a grid cell
    * dataSpace: the Space contains all objects
    * gridsNum: the grid number of dataSpace
    */
  def getZOrderLeftLowPoint(Dim: Int, ZOrderValue: Int, dataSpace: Array[(Double)], gridsNum: Int): Array[(Double)] = {
    //得到该点所在的的网格的左下角的位置坐标
    val zorderLength = (gridsNum - 1).toBinaryString.length / 2
    //    val numSlice = Math.pow(2,zorderLength).toInt
    val numSlice = Math.pow(Dim, zorderLength).toInt
    val zOrderValueBinary = getBinary(ZOrderValue, zorderLength)
    val allDimMinValue = dataSpace.take(Dim)
    val allDimMaxValue = dataSpace.takeRight(Dim)
    val gridLength = allDimMinValue.zip(allDimMaxValue).map(x => {
      val gridLength = (x._2 - x._1) / numSlice
      gridLength
    }) //得到每一维上应该划分的分片大小
    var allDimValue = ""
    for (j <- 0 to Dim - 1) yield {
      for (i <- 0 to zOrderValueBinary.length - 1 if i % Dim == j) yield allDimValue = allDimValue + zOrderValueBinary(i)
      allDimValue += " "
    }
    val allDimtoInt = allDimValue.trim.split(" ").map(x => Integer.parseInt(x, 2)) //得到该ZOrder值所在每一维的值（十进制）
    val LeftPointValue = allDimtoInt.zip(gridLength).map(x => x._1 * x._2)
    LeftPointValue
  }


  /**
    * Dim: the Dim of object
    * ZOrderValue: the ZOrder Value of a grid cell
    * dataSpace: the Space contains all objects
    * gridsNum: the grid number of dataSpace
    */
  def getZOrderRightUpPoint(Dim: Int, ZOrderValue: Int, dataSpace: Array[(Double)], gridsNum: Int): Array[(Double)] = {
    //得到该点所在的的网格的右上角的位置坐标
    val zorderLength = (gridsNum - 1).toBinaryString.length / 2
    //    val numSlice = Math.pow(2,zorderLength).toInt
    val numSlice = Math.pow(Dim, zorderLength).toInt
    val zOrderValueBinary = getBinary(ZOrderValue, zorderLength)
    val allDimMinValue = dataSpace.take(Dim)
    val allDimMaxValue = dataSpace.takeRight(Dim)
    val gridLength = allDimMinValue.zip(allDimMaxValue).map(x => {
      val gridLength = (x._2 - x._1) / numSlice
      gridLength
    }) //得到每一维上应该划分的分片大小
    var allDimValue = ""
    for (j <- 0 to Dim - 1) yield {
      for (i <- 0 to zOrderValueBinary.length - 1 if i % Dim == j) yield allDimValue = allDimValue + zOrderValueBinary(i)
      allDimValue += " "
    }
    val allDimtoInt = allDimValue.trim.split(" ").map(x => Integer.parseInt(x, 2) + 1) //得到该ZOrder值所在每一维的值（十进制）
    val RightPointValue = allDimtoInt.zip(gridLength).map(x => x._1 * x._2)
    RightPointValue
  }

  def getBinary(num: Int, zorderLengh: Int): String = {
    val numToBinary = num.toBinaryString
    if (numToBinary.length < zorderLengh) 1.to(zorderLengh - numToBinary.length).map(_ => "0").foldRight(numToBinary)(_ + _) else numToBinary
  }
}

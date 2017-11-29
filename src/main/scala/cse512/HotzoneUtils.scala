package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    // YOU NEED TO CHANGE THIS PART

    if (queryRectangle == null || pointString == null) {
      return false
    }

    val valsRectangle = queryRectangle.split(",")
    val valsPoint = pointString.split(",")
    if (valsRectangle.length != 4 || valsPoint.length != 2) {
      return false
    }

    val recX1: Double = Math.min(valsRectangle(0).toDouble, valsRectangle(2).toDouble)
    val recY1: Double = Math.min(valsRectangle(1).toDouble, valsRectangle(3).toDouble)
    val recX2: Double = Math.max(valsRectangle(0).toDouble, valsRectangle(2).toDouble)
    val recY2: Double = Math.max(valsRectangle(1).toDouble, valsRectangle(3).toDouble)

    val pointX: Double = valsPoint(0).toDouble
    val pointY: Double = valsPoint(1).toDouble

    if (pointX >= recX1 && pointY >= recY1 && pointX <= recX2 && pointY <= recY2) {
      return true
    }

    return false
  }
}

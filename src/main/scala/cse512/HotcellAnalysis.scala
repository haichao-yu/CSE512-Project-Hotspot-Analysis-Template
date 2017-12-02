package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {

    // Mapper 1: Map each pickup record to a cube, which is represented with (x, y, z).

    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )
    spark.udf.register("CalculateY",(pickupPoint: String)=>
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )
    spark.udf.register("CalculateZ",(pickupTime: String)=>
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5), CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART

    // Reducer 1: Add up pickup records which belong to the same cube (A new column "count" is added to indicate how many pickup records belong to this cube).
    var cubeDf = pickupInfo.groupBy("x", "y", "z").count()
    cubeDf.show()

    // Mapper 2: Map each cube to its 27 neighbors (including itself).
    cubeDf.createOrReplaceTempView("cube")
    spark.udf.register("extendCoordinate", (coordinate: Int) =>
      HotcellUtils.extendCoordinate(coordinate)
    )
    cubeDf = spark.sql("select extendCoordinate(cube.x), extendCoordinate(cube.y), extendCoordinate(cube.z), cube.count from cube")
    cubeDf = cubeDf.toDF(Seq("x", "y", "z", "count"):_*)
    cubeDf.show()

    cubeDf = cubeDf.select(explode(cubeDf("x")).alias("x"), cubeDf("y"), cubeDf("z"), cubeDf("count"))  // explode x
    cubeDf = cubeDf.select(cubeDf("x"), explode(cubeDf("y")).alias("y"), cubeDf("z"), cubeDf("count"))  // explode y
    cubeDf = cubeDf.select(cubeDf("x"), cubeDf("y"), explode(cubeDf("z")).alias("z"), cubeDf("count"))  // explode z
    cubeDf.show()

    // Reducer 2: Add up the count for each cube as the G score
    cubeDf = cubeDf.groupBy("x", "y", "z").agg(sum("count") as "score").sort(desc("score"))

    // Get the top 50 hottest cells (drop the G score column)
    val top50HottestCells = cubeDf.limit(50).drop("score")
    top50HottestCells.show()

    return top50HottestCells
  }
}

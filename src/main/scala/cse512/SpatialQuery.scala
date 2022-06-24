package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def STContains(queryRectangle: String, pointString: String): Boolean = {

      val rectPts = queryRectangle.split(',')
      val x1: Double = rectPts(0).toDouble
      val y1: Double = rectPts(1).toDouble
      val x2: Double = rectPts(2).toDouble
      val y2: Double = rectPts(3).toDouble
      
      val points = pointString.split(',')
      val x: Double = points(0).toDouble
      val y: Double = points(1).toDouble

      var result: Boolean = true

      if(x1 > x2) {
        if((x > x1 || x < x2)) result = false
      } 
      else {
        if((x > x2 || x < x1)) result = false
      }

      if(result != false) {
        if(y1 > y2) {
          if((y > y1 || y < y2)) result = false
        }
        else {
          if((y > y2 || y < y1)) result = false
        }
      }

      return result
  }

def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val ptArraywithin1 = pointString1.split(",")
    val ptArraywithin2 = pointString2.split(",")

    val relx= ptArraywithin1(0).toDouble - ptArraywithin2(0).toDouble
    val rely = ptArraywithin1(1).toDouble - ptArraywithin2(1).toDouble
    val distbetween = math.sqrt(math.pow(relx,2) + math.pow(rely,2))

    if (distbetween <= distance)
    {
      return true
    }
    return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle: String, pointString: String) => STContains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((STContains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}

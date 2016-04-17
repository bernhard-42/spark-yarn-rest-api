import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.SaveMode

case class Iris(sepalLength: Double, sepalWidth:Double, petalLength:Double, petalWidth:Double, species:String)

object IrisApp {

	val hdfsPrefix = ""

 	def main(args: Array[String]) {

		val conf = new SparkConf().setAppName("Iris Application")
		val sc = new SparkContext()
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

		val iris = sc.textFile(hdfsPrefix + "/tmp/iris.data")
					 .filter(row => row.size>0)
		             .map(_.split(","))
		             .map(row => Iris(row(0).trim.toDouble, row(1).trim.toDouble, row(2).trim.toDouble, row(3).trim.toDouble, row(4)))
		             .toDF()
		
		val means = iris.groupBy("species").mean("sepalLength", "sepalWidth", "petalLength", "petalWidth")

 		means.repartition(1)
 		     .write.format("json").mode(SaveMode.Overwrite).save(hdfsPrefix + "/tmp/iris/means")
	}
}
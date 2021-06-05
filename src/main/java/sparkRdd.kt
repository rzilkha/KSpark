import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.Tuple2

fun main(args: Array<String>) {
    var session = SparkSession.builder().appName("test").master("local[*]").orCreate

    var sc = JavaSparkContext(session.sparkContext())

    var scores = sc.textFile("csv_scores.csv").mapToPair {
        var items = it.split(',')
        Tuple2<String, Int>(items[0], Integer.valueOf(items[2]))
    }.reduceByKey { v1, v2 -> v1 + v2 }.collectAsMap()

    sc.textFile("csv_scores.csv").map { {} }
    println(scores)

}
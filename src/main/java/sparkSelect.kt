import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

fun main(args: Array<String>) {

    var session = SparkSession.builder().appName("test").master("local[*]").orCreate

    var df = session.read().format("csv")
            .option("header",true).
                    load("C:\\KotlinSpark\\src\\main\\resources\\csv_example.csv").toDF()
    df.select(col("firstName"), col("lastName"))
        .where(col("firstName").equalTo("roee")).show()

}
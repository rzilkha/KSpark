import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StringType




fun main(args: Array<String>) {
    var something:String? = "hi"
    something="hello"
    val regex = Regex(pattern = "(?<=\\[\\[)(.*?)(?=\\])")
    val udfattempt = UDF1<String,List<String?>>{
        regex.findAll(it).map{it.groupValues[1]}.toList()
    }
    val udfFunc = udf(udfattempt, ArrayType.apply(DataTypes.StringType, false))
    val sparkSession = SparkSession.builder().appName("test").master("spark://10.0.75.1:7077").orCreate
    val df = sparkSession.read().format("com.databricks.spark.xml").option("rowTag","page")
            .load("C:\\KotlinSpark\\src\\main\\resources\\wikipages.xml")
    val raw_pages = df.select(col("title"),col("revision.text._VALUE"))
    val links = raw_pages.withColumn("links",  explode(udfFunc.apply(col("_VALUE"))) )
    links.show()
    sparkSession.stop()
}
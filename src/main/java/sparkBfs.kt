import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df
import org.apache.parquet.example.Paper.schema
import org.apache.parquet.schema.Types
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.map
import org.jetbrains.kotlinx.spark.api.mapPartitions
import org.jetbrains.kotlinx.spark.api.withSpark
import java.io.Serializable
import java.lang.invoke.SerializedLambda


fun main(args: Array<String>) {

    class Something: Serializable, MapFunction<Row, Row> {
        override fun call(value: Row?): Row {
            TODO("Not yet implemented")
        }

    }

    val numbers = listOf(3, 4, 5, 6, 7, 8, 9)

    val aggregated = numbers.groupingBy { it % 3 }.aggregateTo(mutableMapOf()) { key, accumulator: StringBuilder?, element, first ->
        if (first) // first element
            StringBuilder().append(key).append(":").append(element)
        else
            accumulator!!.append("-").append(element)
    }



    withSpark {
        val df = spark.read().format("csv")
            .option("header", true)
            .load("C:\\KotlinSpark\\src\\main\\resources\\connections.csv").toDF()
        val schema: StructType? = StructType()
            .add(StructField.apply("id", DataTypes.StringType, true, null))
            .add(StructField.apply("id2", ArrayType.apply(DataTypes.StringType), true, null))

        val runit = object : MapPartitionsFunction<Row?, Row>, java.io.Serializable {

            override fun call(input: MutableIterator<Row?>?): MutableIterator<Row>? {


                var nameToFriend = input?.asSequence()?.groupingBy { it?.getString(0) }
                    ?.aggregateTo(mutableMapOf()) { key, accumulator: MutableList<String?>?, element, first ->

                        val friend :String? = element?.getString(1)
                        if(first){
                            mutableListOf(friend)
                        }else{
                            accumulator?.add(friend)
                            accumulator
                        }
                    }

                    ?.map { RowFactory.create(it.key, it.value) }?.toMutableList()


                return nameToFriend?.iterator()
            }
        }

        var rowEncoder: ExpressionEncoder<Row> = RowEncoder.apply(schema)
        df.mapPartitions(runit, rowEncoder).show()
    }




   
}


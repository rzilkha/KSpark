
import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df
import org.apache.parquet.example.Paper.schema
import org.apache.parquet.filter.ColumnRecordFilter.column
import org.apache.parquet.schema.Types
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.*

import org.apache.spark.sql.functions.arrays_zip

import org.apache.spark.sql.functions.flatten


import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.eq
import org.jetbrains.kotlinx.spark.api.withSpark

import java.io.Serializable
import java.lang.invoke.SerializedLambda




fun main(args: Array<String>) {

    withSpark {
        val df = spark.read().format("csv")
            .option("header", true)
            .load("C:\\KotlinSpark\\src\\main\\resources\\connections.csv").toDF()
        val schema: StructType? = StructType()
            .add(StructField.apply("name", DataTypes.StringType, true, null))
            .add(StructField.apply("connections", ArrayType.apply(DataTypes.StringType), true, null))
            .add(StructField.apply("distance", DataTypes.StringType, true, null))
            .add(StructField.apply("status", DataTypes.IntegerType, true, null))

        val normalizedValueFunction = object : MapPartitionsFunction<Row?, Row>, java.io.Serializable {

            override fun call(input: MutableIterator<Row?>?): MutableIterator<Row>? {
                var nameToFriend = input?.asSequence()?.groupingBy { it?.getString(0) }
                    ?.aggregateTo(mutableMapOf()) { key, accumulator: MutableList<String?>?, element, first ->

                        val friend: String? = element?.getString(1)
                        if (first) {
                            mutableListOf(friend)
                        } else {
                            accumulator?.add(friend)
                            accumulator
                        }
                    }

                    ?.map {
                        val status = if (it.key.equals("roee")) 1 else 0
                        RowFactory.create(it.key, it.value?.toTypedArray(), null, status)
                    }?.toMutableList()


                return nameToFriend?.iterator()
            }
        }

        val rowEncoder: ExpressionEncoder<Row> = RowEncoder.apply(schema)

        //df.mapPartitions(normalizedValueFunction, rowEncoder)
        var data = df.select(col("name"), col("friend"))
            .groupBy(col("name"))
            .agg(collect_list(col("friend")).`as`("connections"))
            .withColumn("status", `when`(col("name").equalTo("roee"), 1).otherwise(0).`as`("status"))
            .withColumn("distance", lit(null))

        for (i in 1..4) {

            // set all rows that are with status ready to contain the distance
            val reduced =
                data.where(col("status").equalTo(1))
                    .select(
                        explode(col("connections")).`as`("name"),
                        lit(array()).`as`("connections"), lit(i).`as`("distance"),
                        lit(1).`as`("status")
                    )


            // change all that was ready to done( status=2)
            val convertToDone = data.select(
                col("name"), col("connections"), col("distance"),
                `when`(col("status").equalTo(1), 2).otherwise(col("status")).`as`("status")
            )


            val unionWithConnections = convertToDone.union(reduced)


            //  group all ready rows with other rows, grouped by name
            data = unionWithConnections.select("*").groupBy(col("name"))
                .agg(
                    min(col("distance")).`as`("distance"),
                    flatten(collect_list(col("connections"))).`as`("connections"),
                    max(col("status")).`as`("status")
                )
            data.show()
//        }

        }
    }
}


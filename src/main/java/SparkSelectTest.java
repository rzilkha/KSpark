import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class SparkSelectTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("try").master("local[*]").getOrCreate();

        Dataset<Row> df = session.read().format("csv").option("header",true).load("C:\\KotlinSpark\\src\\main\\resources\\csv_example.csv").toDF();

        df.select(col("firstName"),col("lastName"), col("age").minus(10).alias("ageMinus10")).show();

    }
}

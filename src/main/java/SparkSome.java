import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSome {

    public static void main(String[] args) {


        SparkSession session = SparkSession.builder().appName("try").master("local[*]").getOrCreate();

        Dataset<Row> df = session.read().format("csv").option("header",true).load("C:\\KotlinSpark\\src\\main\\resources\\csv_example.csv").toDF();



        StructType schema = new StructType()
                .add(StructField.apply("id", DataTypes.StringType, true, null))
                .add(StructField.apply("id2", DataTypes.StringType, true, null));
        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(schema);


//        Encoder<Tuple2<String, String>> hiso = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
//        MapFunction<Row, Row> wh = value -> RowFactory.create("hi","man");
//        MapFunction<Row, Row> run;
//        df.map(wh, rowEncoder).show();

        MapPartitionsFunction<Row, Row> mpf = value -> {
            Row row = RowFactory.create("hi", "man");
            Row row2 = RowFactory.create("hi", "man2");
            List<Row> list = Lists.newArrayList();
            list.add(row);
            list.add(row2);
            return list.iterator();
        };

        df.mapPartitions(mpf, rowEncoder).show();
    }
}

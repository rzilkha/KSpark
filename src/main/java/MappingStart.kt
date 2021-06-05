import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory

class MappingStart : MapFunction<Row?, Row> {
    override fun call(value: Row?): Row {
        return RowFactory.create("hi", "john")
    }
}
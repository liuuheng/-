package notes.spark.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

/** 与同名 Hive 学习案例保持业务规则；采用 Spark 原生接口。 */
public class SparkTransArray implements UDF3<String, String, Seq<String>, List<Row>> {
    public static ArrayType resultType() {
        StructType row = new StructType()
                .add("row_key", DataTypes.StringType, true)
                .add("parts", DataTypes.createArrayType(DataTypes.StringType, true), false);
        return DataTypes.createArrayType(row, false);
    }

    @Override
    public List<Row> call(String key, String separator, Seq<String> columns) {
        if (separator == null || separator.isEmpty()) throw new IllegalArgumentException("Non-empty separator required");
        if (columns == null || columns.size() == 0) throw new IllegalArgumentException("At least one data column required");
        Pattern splitPattern = Pattern.compile(Pattern.quote(separator));
        String[][] parts = new String[columns.size()][];
        int rows = 0;
        for (int column = 0; column < columns.size(); column++) {
            String value = columns.apply(column);
            if (value != null) {
                parts[column] = splitPattern.split(value, -1);
                rows = Math.max(rows, parts[column].length);
            }
        }
        List<Row> result = new ArrayList<>();
        for (int index = 0; index < Math.max(1, rows); index++) {
            String[] aligned = new String[columns.size()];
            for (int column = 0; column < columns.size(); column++) {
                if (parts[column] != null && index < parts[column].length) aligned[column] = parts[column][index];
            }
            result.add(RowFactory.create(key, Arrays.asList(aligned)));
        }
        return result;
    }
}

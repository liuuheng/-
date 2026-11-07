package notes.flink.udf;

import java.util.regex.Pattern;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.types.Row;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkTransArray extends TableFunction<Row> {
    @FunctionHint(output = @DataTypeHint("ROW<row_key STRING, parts ARRAY<STRING>>"))
    public void eval(String key, String separator, String[] columns) {
        if (separator == null || separator.isEmpty()) throw new IllegalArgumentException("Non-empty separator required");
        if (columns == null || columns.length == 0) throw new IllegalArgumentException("At least one data column required");
        Pattern splitPattern = Pattern.compile(Pattern.quote(separator));
        String[][] parts = new String[columns.length][];
        int rows = 0;
        for (int column = 0; column < columns.length; column++) {
            if (columns[column] != null) {
                parts[column] = splitPattern.split(columns[column], -1);
                rows = Math.max(rows, parts[column].length);
            }
        }
        for (int index = 0; index < Math.max(1, rows); index++) {
            String[] aligned = new String[columns.length];
            for (int column = 0; column < columns.length; column++) {
                if (parts[column] != null && index < parts[column].length) aligned[column] = parts[column][index];
            }
            collect(Row.of(key, aligned));
        }
    }
}

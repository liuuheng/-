package notes.spark.udf;

import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.sql.api.java.UDF4;

/** 与同名 Hive 学习案例保持业务规则；采用 Spark 原生接口。 */
public class SparkSplitPart implements UDF4<String, String, Integer, Integer, String> {
    public String call(String source, String separator, Integer start, Integer end) {
        if (source == null || separator == null || start == null || end == null || start < 1) return null;
        if (end < start) throw new IllegalArgumentException("end must be at least start");
        if (separator.isEmpty()) return source;
        String[] parts = source.split(Pattern.quote(separator), -1);
        if (start > parts.length) return "";
        return String.join(separator, Arrays.copyOfRange(parts, start - 1, Math.min(end, parts.length)));
    }
}

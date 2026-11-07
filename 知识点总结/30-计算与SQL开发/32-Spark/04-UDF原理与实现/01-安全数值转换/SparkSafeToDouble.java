package notes.spark.udf;

import java.lang.Double;
import org.apache.spark.sql.api.java.UDF1;

/** 与同名 Hive 学习案例保持业务规则；采用 Spark 原生接口。 */
public class SparkSafeToDouble implements UDF1<String, Double> {
    public Double call(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            double number = Double.parseDouble(value);
            return Double.isFinite(number) ? number : null;
        } catch (NumberFormatException invalidNumber) {
            return null;
        }
    }
}

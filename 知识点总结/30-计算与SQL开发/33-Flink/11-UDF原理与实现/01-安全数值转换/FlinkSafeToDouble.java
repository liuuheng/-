package notes.flink.udf;

import java.lang.Double;
import org.apache.flink.table.functions.ScalarFunction;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkSafeToDouble extends ScalarFunction {
    public Double eval(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            double number = Double.parseDouble(value);
            return Double.isFinite(number) ? number : null;
        } catch (NumberFormatException invalidNumber) {
            return null;
        }
    }
}

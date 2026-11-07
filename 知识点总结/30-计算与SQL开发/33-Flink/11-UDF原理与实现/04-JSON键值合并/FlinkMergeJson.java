package notes.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import com.alibaba.fastjson.JSONObject;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkMergeJson extends ScalarFunction {
    public String eval(String... pairs) {
        if (pairs == null) return null;
        if (pairs.length == 0 || pairs.length % 2 != 0) throw new IllegalArgumentException("Non-empty key/value pairs required");
        JSONObject result = new JSONObject(true);
        for (int index = 0; index < pairs.length; index += 2) {
            if (pairs[index] != null && pairs[index + 1] != null) result.put(pairs[index], pairs[index + 1]);
        }
        return result.toJSONString();
    }
}

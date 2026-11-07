package notes.spark.udf;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.Seq;
import com.alibaba.fastjson.JSONObject;

/** 与同名 Hive 学习案例保持业务规则；采用 Spark 原生接口。 */
public class SparkMergeJson implements UDF1<Seq<String>, String> {
    @Override
    public String call(Seq<String> pairs) {
        if (pairs == null) return null;
        if (pairs.size() == 0 || pairs.size() % 2 != 0) throw new IllegalArgumentException("Non-empty key/value pairs required");
        JSONObject result = new JSONObject(true);
        for (int index = 0; index < pairs.size(); index += 2) {
            String key = pairs.apply(index), value = pairs.apply(index + 1);
            if (key != null && value != null) result.put(key, value);
        }
        return result.toJSONString();
    }
}

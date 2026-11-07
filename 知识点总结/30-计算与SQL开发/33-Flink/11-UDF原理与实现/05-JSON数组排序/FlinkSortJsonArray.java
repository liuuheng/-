package notes.flink.udf;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkSortJsonArray extends ScalarFunction {
    private static final class Item {
        final JSONObject value;
        final BigDecimal key;
        Item(JSONObject value, String field) {
            this.value = value;
            Object key = value.get(field);
            this.key = key == null ? null : new BigDecimal(key.toString());
        }
    }

    public String eval(String input, String field, Boolean ascending) {
        if (input == null || field == null || ascending == null) return null;
        JSONArray array;
        try { array = JSONArray.parseArray(input); }
        catch (JSONException invalidJson) { return null; }
        if (array == null) return null;
        List<Item> items = new ArrayList<>(array.size());
        for (Object value : array) {
            if (!(value instanceof JSONObject)) throw new IllegalArgumentException("Array elements must be objects");
            items.add(new Item((JSONObject) value, field));
        }
        items.sort((left, right) -> {
            if (left.key == null || right.key == null) return left.key == right.key ? 0 : left.key == null ? 1 : -1;
            return ascending ? left.key.compareTo(right.key) : right.key.compareTo(left.key);
        });
        JSONArray result = new JSONArray(items.size());
        for (Item item : items) result.add(item.value);
        return result.toJSONString();
    }
}

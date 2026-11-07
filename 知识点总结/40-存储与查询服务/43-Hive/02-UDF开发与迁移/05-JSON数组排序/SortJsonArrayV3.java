/**
 * dt_udf_name: sort_json_array
 * SortJsonArray.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.customize;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

/** 按数值字段稳定排序，字段缺失或为 JSON null 时统一排在末尾。 */
public class SortJsonArrayV3 extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;

    private static final class Row {
        final JSONObject object;
        final BigDecimal key;

        Row(JSONObject object, String field) {
            this.object = object;
            Object value = object.get(field);
            this.key = value == null ? null : new BigDecimal(value.toString());
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 3, 3);
        PrimitiveCategory[] inputTypes = new PrimitiveCategory[3];
        converters = new ObjectInspectorConverters.Converter[3];
        obtainStringConverter(arguments, 0, inputTypes, converters);
        obtainStringConverter(arguments, 1, inputTypes, converters);
        if (!(arguments[2] instanceof BooleanObjectInspector)) {
            throw new UDFArgumentException("Ascending flag must be BOOLEAN");
        }
        converters[2] = ObjectInspectorConverters.getConverter(
                arguments[2], PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String input = getStringValue(arguments, 0, converters);
        String field = getStringValue(arguments, 1, converters);
        BooleanWritable ascending = (BooleanWritable) converters[2].convert(arguments[2].get());
        if (input == null || field == null || ascending == null) {
            return null;
        }
        try {
            String result = calculate(input, field, ascending.get());
            return result == null ? null : new Text(result);
        } catch (RuntimeException invalidInput) {
            throw new HiveException("sort_json_array: invalid array element or numeric field", invalidInput);
        }
    }

    public String calculate(String input, String field, Boolean ascending) {
        if (input == null || field == null || ascending == null) {
            return null;
        }
        JSONArray source;
        try {
            source = JSONArray.parseArray(input);
        } catch (JSONException invalidJson) {
            return null;
        }
        if (source == null) {
            return null;
        }

        // 先计算一次排序键，再执行多次比较，避免在比较器中重复转换。
        List<Row> rows = new ArrayList<>(source.size());
        for (int index = 0; index < source.size(); index++) {
            Object element = source.get(index);
            if (!(element instanceof JSONObject)) {
                throw new IllegalArgumentException("Every array element must be a JSON object");
            }
            rows.add(new Row((JSONObject) element, field));
        }
        rows.sort((left, right) -> {
            if (left.key == null || right.key == null) {
                return left.key == right.key ? 0 : left.key == null ? 1 : -1;
            }
            return ascending ? left.key.compareTo(right.key) : right.key.compareTo(left.key);
        });

        JSONArray result = new JSONArray(rows.size());
        for (Row row : rows) {
            result.add(row.object);
        }
        return result.toJSONString();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("sort_json_array", children);
    }
}

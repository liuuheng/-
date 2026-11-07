/**
 * dt_udf_name: merge_json_udf
 * MergeJsonUDF.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.customize.py;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.Text;

/** 将字符串键值对组成 JSON；跳过 NULL 对，重复键采用后面的有效值。 */
public class MergeJsonUDF extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 0 || arguments.length % 2 != 0) {
            throw new UDFArgumentException("Non-empty key/value pairs are required");
        }
        PrimitiveCategory[] inputTypes = new PrimitiveCategory[arguments.length];
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int index = 0; index < arguments.length; index++) {
            checkArgGroups(arguments, index, inputTypes, PrimitiveGrouping.STRING_GROUP, PrimitiveGrouping.VOID_GROUP);
            obtainStringConverter(arguments, index, inputTypes, converters);
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 每行使用独立对象，避免前一行的键值污染当前行。
        JSONObject result = new JSONObject(true);
        for (int index = 0; index < arguments.length; index += 2) {
            String key = getStringValue(arguments, index, converters);
            String value = getStringValue(arguments, index + 1, converters);
            if (key != null && value != null) {
                result.put(key, value);
            }
        }
        return new Text(result.toJSONString());
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("merge_json_udf", children);
    }
}

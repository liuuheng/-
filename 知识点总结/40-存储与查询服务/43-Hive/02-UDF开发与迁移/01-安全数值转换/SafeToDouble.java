/**
 * dt_udf_name: safe_to_double
 * SafeToDouble.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.customize;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/** 将字符串转为有限 double；缺失、非法数字及非有限值返回 NULL。 */
public class SafeToDouble extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);
        PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
        converters = new ObjectInspectorConverters.Converter[1];
        obtainStringConverter(arguments, 0, inputTypes, converters);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String value = getStringValue(arguments, 0, converters);
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            double number = Double.parseDouble(value);
            return Double.isFinite(number) ? new DoubleWritable(number) : null;
        } catch (NumberFormatException invalidNumber) {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("safe_to_double", children);
    }
}

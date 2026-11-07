/**
 * dt_udf_name: split_part
 * SplitPartUDF.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.odps.udf;

import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/** 使用字面分隔符提取一基闭区间，保留尾部空段。 */
public class SplitPartUDF extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 3, 4);
        PrimitiveCategory[] inputTypes = new PrimitiveCategory[arguments.length];
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        obtainStringConverter(arguments, 0, inputTypes, converters);
        obtainStringConverter(arguments, 1, inputTypes, converters);
        obtainIntConverter(arguments, 2, inputTypes, converters);
        if (arguments.length == 4) {
            obtainIntConverter(arguments, 3, inputTypes, converters);
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String source = getStringValue(arguments, 0, converters);
        String separator = getStringValue(arguments, 1, converters);
        Integer start = getIntValue(arguments, 2, converters);
        Integer end = arguments.length == 4 ? getIntValue(arguments, 3, converters) : start;
        if (source == null || separator == null || start == null || end == null || start < 1) {
            return null;
        }
        if (end < start) {
            throw new HiveException("end must be greater than or equal to start");
        }
        if (separator.isEmpty()) {
            return new Text(source);
        }

        String[] parts = source.split(Pattern.quote(separator), -1);
        if (start > parts.length) {
            return new Text("");
        }
        String[] selected = Arrays.copyOfRange(parts, start - 1, Math.min(end, parts.length));
        return new Text(String.join(separator, selected));
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("split_part", children);
    }
}

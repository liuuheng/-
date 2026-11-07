/*
 * RegexpExtractAllToArrayUDF.java
 * Copyright 2022 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.customize;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/** 提取所有完整匹配，返回带尾部分隔符的 STRING；类名沿用项目中的命名。 */
public class RegexpExtractAllToArrayUDF extends GenericUDF {
    private transient String cachedRegex;
    private transient Pattern cachedPattern;
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 2, 3);
        PrimitiveCategory[] inputTypes = new PrimitiveCategory[arguments.length];
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int index = 0; index < arguments.length; index++) {
            obtainStringConverter(arguments, index, inputTypes, converters);
        }
        cachedRegex = null;
        cachedPattern = null;
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String source = getStringValue(arguments, 0, converters);
        String regex = getStringValue(arguments, 1, converters);
        String delimiter = arguments.length == 3 ? getStringValue(arguments, 2, converters) : ",";
        if (source == null || regex == null || delimiter == null) {
            return null;
        }
        try {
            // 单项缓存避免相同模式逐行编译，也避免缓存无限增长。
            if (!regex.equals(cachedRegex)) {
                cachedPattern = Pattern.compile(regex, Pattern.DOTALL);
                cachedRegex = regex;
            }
        } catch (PatternSyntaxException invalidPattern) {
            throw new HiveException("Invalid regular expression", invalidPattern);
        }

        Matcher matcher = cachedPattern.matcher(source);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            result.append(matcher.group()).append(delimiter);
        }
        return result.length() == 0 ? null : new Text(result.toString());
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("regexp_extract_all_text", children);
    }
}

/**
 * dt_udf_name: trans_array
 * TransArrayUDTF.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.odps.udtf;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/** 将多个分隔字符串按位置展开，保留键列，并用 NULL 补齐短列。 */
public class TransArrayUDTFV4 extends GenericUDTF {
    private transient ObjectInspector[] inputInspectors;
    private transient StructObjectInspector outputInspector;
    private transient Pattern separatorPattern;
    private int keyCount;
    private int dataColumnCount;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments == null || arguments.length < 3) {
            throw new UDFArgumentException("Expected key count, separator and at least one data column");
        }
        if (!(arguments[0] instanceof ConstantObjectInspector)
                || !"int".equals(arguments[0].getTypeName())) {
            throw new UDFArgumentException("Key count must be an INT constant");
        }
        Object keyConstant = ((ConstantObjectInspector) arguments[0]).getWritableConstantValue();
        if (keyConstant == null) {
            throw new UDFArgumentException("Key count must not be NULL");
        }
        keyCount = ((IntWritable) keyConstant).get();
        if (keyCount < 0 || keyCount >= arguments.length - 2) {
            throw new UDFArgumentException("Key count must leave at least one data column");
        }
        if (!(arguments[1] instanceof ConstantObjectInspector)
                || !(arguments[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("Separator must be a STRING constant");
        }
        Object separatorConstant = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
        if (separatorConstant == null || separatorConstant.toString().isEmpty()) {
            throw new UDFArgumentException("Separator must not be NULL or empty");
        }
        separatorPattern = Pattern.compile(Pattern.quote(separatorConstant.toString()));
        inputInspectors = arguments.clone();
        dataColumnCount = arguments.length - 2 - keyCount;

        List<String> names = new ArrayList<>();
        List<ObjectInspector> types = new ArrayList<>();
        for (int index = 2; index < arguments.length; index++) {
            names.add("c" + (index - 2));
            if (index < keyCount + 2) {
                if (!(arguments[index] instanceof PrimitiveObjectInspector)) {
                    throw new UDFArgumentException("Key columns must have primitive types");
                }
                types.add(ObjectInspectorUtils.getStandardObjectInspector(
                        arguments[index], ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE));
            } else {
                if (!(arguments[index] instanceof StringObjectInspector)) {
                    throw new UDFArgumentException("Data columns must have STRING type");
                }
                types.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            }
        }
        outputInspector = ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
        return outputInspector;
    }

    @Override
    public void process(Object[] arguments) throws HiveException {
        if (inputInspectors == null || arguments == null || arguments.length != inputInspectors.length) {
            throw new HiveException("Function must be initialized with the same argument count");
        }
        try {
            Object[] keys = new Object[keyCount];
            for (int index = 0; index < keyCount; index++) {
                keys[index] = ((PrimitiveObjectInspector) inputInspectors[index + 2])
                        .getPrimitiveWritableObject(arguments[index + 2]);
            }

            // 每个待展开的字符串只拆分一次。
            String[][] parts = new String[dataColumnCount][];
            int maximumRows = 0;
            for (int column = 0; column < dataColumnCount; column++) {
                int inputIndex = keyCount + 2 + column;
                String value = ((StringObjectInspector) inputInspectors[inputIndex])
                        .getPrimitiveJavaObject(arguments[inputIndex]);
                if (value != null) {
                    parts[column] = separatorPattern.split(value, -1);
                    maximumRows = Math.max(maximumRows, parts[column].length);
                }
            }

            // 全部数据列均为 NULL 时仍输出一行，保留键列。
            for (int rowIndex = 0; rowIndex < Math.max(1, maximumRows); rowIndex++) {
                Object[] row = new Object[keyCount + dataColumnCount];
                System.arraycopy(keys, 0, row, 0, keyCount);
                for (int column = 0; column < dataColumnCount; column++) {
                    if (parts[column] != null && rowIndex < parts[column].length) {
                        row[keyCount + column] = new Text(parts[column][rowIndex]);
                    }
                }
                // 不让下游持有仍可能被复用的 Writable 对象。
                List<?> snapshot = (List<?>) ObjectInspectorUtils.copyToStandardObject(
                        row, outputInspector, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
                forward(snapshot.toArray());
            }
        } catch (RuntimeException invalidInput) {
            throw new HiveException("trans_array: invalid input", invalidInput);
        }
    }

    @Override
    public void close() {
        // 无跨行缓冲或外部资源需要释放。
    }
}

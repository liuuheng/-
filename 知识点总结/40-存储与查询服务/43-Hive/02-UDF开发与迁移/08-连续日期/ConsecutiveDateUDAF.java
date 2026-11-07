/*
 * ConsecutiveDate.java
 * Copyright 2025 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.odps.udaf;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/** 计算有效 yyyyMMdd 日期去重后的最长连续天数。 */
public class ConsecutiveDateUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] types) throws SemanticException {
        if (types.length != 1 || !"string".equals(types[0].getTypeName())) {
            throw new SemanticException("Exactly one STRING date is required");
        }
        return new ConsecutiveDateEvaluator();
    }

    public static class ConsecutiveDateEvaluator extends GenericUDAFEvaluator {
        private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("uuuuMMdd")
                .withResolverStyle(ResolverStyle.STRICT);
        private transient PrimitiveObjectInspector inputInspector;
        private transient ListObjectInspector partialInspector;

        public static class State extends AbstractAggregationBuffer {
            final SortedSet<Long> days = new TreeSet<>();
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputInspector = (PrimitiveObjectInspector) parameters[0];
            } else {
                partialInspector = (ListObjectInspector) parameters[0];
            }
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            }
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer aggregation) {
            ((State) aggregation).days.clear();
        }

        @Override
        public void iterate(AggregationBuffer aggregation, Object[] row) {
            if (row[0] == null) {
                return;
            }
            String value = PrimitiveObjectInspectorUtils.getString(row[0], inputInspector);
            if (value == null || value.length() != 8) {
                return;
            }
            try {
                long epochDay = LocalDate.parse(value, FORMAT).toEpochDay();
                ((State) aggregation).days.add(epochDay);
            } catch (DateTimeParseException invalidDate) {
                // 数据缺失或日历日期非法时忽略该行。
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregation) {
            List<LongWritable> snapshot = new ArrayList<>();
            for (long day : ((State) aggregation).days) {
                snapshot.add(new LongWritable(day));
            }
            return snapshot;
        }

        @Override
        public void merge(AggregationBuffer aggregation, Object partial) {
            if (partial == null) {
                return;
            }
            PrimitiveObjectInspector elementInspector =
                    (PrimitiveObjectInspector) partialInspector.getListElementObjectInspector();
            for (Object day : partialInspector.getList(partial)) {
                if (day != null) {
                    ((State) aggregation).days.add(
                            PrimitiveObjectInspectorUtils.getLong(day, elementInspector));
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregation) {
            int longest = 0;
            int current = 0;
            Long previous = null;
            for (long day : ((State) aggregation).days) {
                current = previous != null && day == previous + 1 ? current + 1 : 1;
                longest = Math.max(longest, current);
                previous = day;
            }
            return new IntWritable(longest);
        }
    }
}

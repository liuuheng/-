/**
 * dt_udf_name: count_if
 * CountIfUDAF.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.hive.udf.odps.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/** 只统计 TRUE；中间结果和最终结果均为 BIGINT。 */
public class MCCountIfUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] types) throws SemanticException {
        if (types.length != 1 || !"boolean".equals(types[0].getTypeName())) {
            throw new SemanticException("Exactly one BOOLEAN argument is required");
        }
        return new CountIfEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return getEvaluator(info.getParameters());
    }

    public static class CountIfEvaluator extends GenericUDAFEvaluator {
        private transient BooleanObjectInspector inputInspector;
        private transient LongObjectInspector partialInspector;

        public static class CountIfAggregationBuffer extends AbstractAggregationBuffer {
            long count;
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (parameters.length != 1) {
                throw new HiveException("count_if requires one argument");
            }
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputInspector = (BooleanObjectInspector) parameters[0];
            } else {
                partialInspector = (LongObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            return new CountIfAggregationBuffer();
        }

        @Override
        public void reset(AggregationBuffer aggregation) {
            ((CountIfAggregationBuffer) aggregation).count = 0;
        }

        private void add(AggregationBuffer aggregation, long delta) throws HiveException {
            CountIfAggregationBuffer state = (CountIfAggregationBuffer) aggregation;
            try {
                state.count = Math.addExact(state.count, delta);
            } catch (ArithmeticException overflow) {
                throw new HiveException("count_if exceeds BIGINT range", overflow);
            }
        }

        @Override
        public void iterate(AggregationBuffer aggregation, Object[] row) throws HiveException {
            if (row[0] != null && inputInspector.get(row[0])) {
                add(aggregation, 1);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregation) {
            return new LongWritable(((CountIfAggregationBuffer) aggregation).count);
        }

        @Override
        public void merge(AggregationBuffer aggregation, Object partial) throws HiveException {
            if (partial != null) {
                add(aggregation, partialInspector.get(partial));
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregation) {
            return new LongWritable(((CountIfAggregationBuffer) aggregation).count);
        }
    }
}

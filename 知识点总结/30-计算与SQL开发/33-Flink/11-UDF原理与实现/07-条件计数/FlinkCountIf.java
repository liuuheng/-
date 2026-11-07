package notes.flink.udf;

import org.apache.flink.table.functions.AggregateFunction;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkCountIf extends AggregateFunction<Long, FlinkCountIf.Accumulator> {
    public static class Accumulator {
        public long count;
    }

    @Override public Accumulator createAccumulator() { return new Accumulator(); }
    @Override public Long getValue(Accumulator accumulator) { return accumulator.count; }

    public void accumulate(Accumulator accumulator, Boolean value) {
        if (Boolean.TRUE.equals(value)) accumulator.count = Math.addExact(accumulator.count, 1L);
    }

    public void retract(Accumulator accumulator, Boolean value) {
        if (Boolean.TRUE.equals(value)) {
            if (accumulator.count == 0) throw new IllegalStateException("Retraction without matching TRUE input");
            accumulator.count--;
        }
    }

    public void merge(Accumulator accumulator, Iterable<Accumulator> others) {
        for (Accumulator other : others) accumulator.count = Math.addExact(accumulator.count, other.count);
    }

    public void resetAccumulator(Accumulator accumulator) { accumulator.count = 0; }
}

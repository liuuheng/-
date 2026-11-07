package notes.spark.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/** 只统计 TRUE；分区内累加与分区间合并采用同一溢出检查。 */
public class SparkCountIf extends Aggregator<Boolean, Long, Long> {
    @Override public Long zero() { return 0L; }

    @Override public Long reduce(Long count, Boolean value) {
        return Boolean.TRUE.equals(value) ? Math.addExact(count, 1L) : count;
    }

    @Override public Long merge(Long left, Long right) {
        return Math.addExact(left, right);
    }

    @Override public Long finish(Long count) { return count; }
    @Override public Encoder<Long> bufferEncoder() { return Encoders.LONG(); }
    @Override public Encoder<Long> outputEncoder() { return Encoders.LONG(); }
}

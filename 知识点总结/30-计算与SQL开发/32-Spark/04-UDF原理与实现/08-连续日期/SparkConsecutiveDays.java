package notes.spark.udf;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/** 以日期集合合并分区状态；用于追加式分组聚合，不实现撤回。 */
public class SparkConsecutiveDays extends Aggregator<String, SparkConsecutiveDays.Buffer, Integer> {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("uuuuMMdd")
            .withResolverStyle(ResolverStyle.STRICT);

    /** Map 的键表示日期集合；使用 Java Bean 编码，值恒为 TRUE。 */
    public static class Buffer implements Serializable {
        private Map<Long, Boolean> days = new HashMap<>();
        public Buffer() { }
        public Map<Long, Boolean> getDays() { return days; }
        public void setDays(Map<Long, Boolean> days) { this.days = new HashMap<>(days); }
    }

    @Override public Buffer zero() { return new Buffer(); }

    private static Long parseDay(String value) {
        if (value == null || value.length() != 8) return null;
        try { return LocalDate.parse(value, FORMAT).toEpochDay(); }
        catch (DateTimeParseException invalidDate) { return null; }
    }

    @Override public Buffer reduce(Buffer buffer, String value) {
        Long day = parseDay(value);
        if (day != null) buffer.getDays().put(day, true);
        return buffer;
    }

    @Override public Buffer merge(Buffer left, Buffer right) {
        left.getDays().putAll(right.getDays());
        return left;
    }

    @Override public Integer finish(Buffer buffer) {
        int current = 0, longest = 0;
        Long previous = null;
        for (long day : new TreeSet<>(buffer.getDays().keySet())) {
            current = previous != null && day == previous + 1 ? current + 1 : 1;
            longest = Math.max(longest, current);
            previous = day;
        }
        return longest;
    }

    @Override public Encoder<Buffer> bufferEncoder() { return Encoders.bean(Buffer.class); }
    @Override public Encoder<Integer> outputEncoder() { return Encoders.INT(); }
}

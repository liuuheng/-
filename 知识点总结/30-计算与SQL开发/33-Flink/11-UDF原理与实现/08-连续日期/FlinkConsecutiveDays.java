package notes.flink.udf;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.table.functions.AggregateFunction;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkConsecutiveDays extends AggregateFunction<Integer, FlinkConsecutiveDays.Accumulator> {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("uuuuMMdd")
            .withResolverStyle(ResolverStyle.STRICT);

    public static class Accumulator {
        public Map<Long, Long> frequencies = new HashMap<>();
    }

    private static Long parseDay(String value) {
        if (value == null || value.length() != 8) return null;
        try { return LocalDate.parse(value, FORMAT).toEpochDay(); }
        catch (DateTimeParseException invalidDate) { return null; }
    }

    @Override public Accumulator createAccumulator() { return new Accumulator(); }

    public void accumulate(Accumulator accumulator, String value) {
        Long day = parseDay(value);
        if (day != null) accumulator.frequencies.merge(day, 1L, Math::addExact);
    }

    public void retract(Accumulator accumulator, String value) {
        Long day = parseDay(value);
        if (day == null) return;
        Long count = accumulator.frequencies.get(day);
        if (count == null) throw new IllegalStateException("Retraction without matching date input");
        if (count == 1) accumulator.frequencies.remove(day);
        else accumulator.frequencies.put(day, count - 1);
    }

    public void merge(Accumulator accumulator, Iterable<Accumulator> others) {
        for (Accumulator other : others) {
            for (Map.Entry<Long, Long> entry : other.frequencies.entrySet()) {
                accumulator.frequencies.merge(entry.getKey(), entry.getValue(), Math::addExact);
            }
        }
    }

    @Override public Integer getValue(Accumulator accumulator) {
        int current = 0, longest = 0;
        Long previous = null;
        for (long day : new TreeSet<>(accumulator.frequencies.keySet())) {
            current = previous != null && day == previous + 1 ? current + 1 : 1;
            longest = Math.max(longest, current);
            previous = day;
        }
        return longest;
    }

    public void resetAccumulator(Accumulator accumulator) { accumulator.frequencies.clear(); }
}

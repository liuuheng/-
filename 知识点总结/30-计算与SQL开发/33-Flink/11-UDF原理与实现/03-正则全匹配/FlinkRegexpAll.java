package notes.flink.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.functions.ScalarFunction;

/** 与同名 Hive 学习案例保持业务规则；采用 Flink 原生接口。 */
public class FlinkRegexpAll extends ScalarFunction {
    private transient String cachedRegex;
    private transient Pattern cachedPattern;

    public String eval(String source, String regex, String delimiter) {
        if (source == null || regex == null || delimiter == null) return null;
        if (!regex.equals(cachedRegex)) {
            cachedPattern = Pattern.compile(regex, Pattern.DOTALL);
            cachedRegex = regex;
        }
        Matcher matcher = cachedPattern.matcher(source);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) result.append(matcher.group()).append(delimiter);
        return result.length() == 0 ? null : result.toString();
    }

    public String eval(String source, String regex) {
        return eval(source, regex, ",");
    }
}

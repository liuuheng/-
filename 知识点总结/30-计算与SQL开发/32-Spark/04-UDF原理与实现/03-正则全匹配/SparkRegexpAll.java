package notes.spark.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.api.java.UDF3;

/** 与同名 Hive 学习案例保持业务规则；采用 Spark 原生接口。 */
public class SparkRegexpAll implements UDF3<String, String, String, String> {
    private transient String cachedRegex;
    private transient Pattern cachedPattern;

    public String call(String source, String regex, String delimiter) {
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
}

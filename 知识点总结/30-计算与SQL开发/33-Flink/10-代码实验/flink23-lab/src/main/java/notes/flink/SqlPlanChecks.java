package notes.flink;

import org.apache.flink.table.api.*;
import java.nio.file.*;
import java.io.IOException;

/** Planner checks only; never submits these streaming jobs. */
public class SqlPlanChecks {
    static int count;
    static void plan(TableEnvironment t,String label,String sql) {
        String explanation=t.explainSql(sql);
        if(!explanation.contains("Execution Plan")) throw new AssertionError(label);
        try {
            Path directory=Path.of(System.getProperty("flink.lab.planDir","target/plans"));
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(label+".txt"),explanation);
        } catch(IOException ex) { throw new RuntimeException(ex); }
        System.out.println("PLAN PASS "+label);
        count++;
    }
    public static void main(String[] args) {
        TableEnvironment t=TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.executeSql("CREATE TABLE events (event_id BIGINT,user_id BIGINT,url STRING,amount BIGINT,"
            +"ts_ms BIGINT,et AS TO_TIMESTAMP_LTZ(ts_ms,3),pt AS PROCTIME(),"
            +"WATERMARK FOR et AS et - INTERVAL '2' SECOND) WITH ("
            +"'connector'='datagen','rows-per-second'='10','number-of-rows'='100',"
            +"'fields.event_id.kind'='sequence','fields.event_id.start'='1','fields.event_id.end'='100',"
            +"'fields.user_id.min'='1','fields.user_id.max'='5',"
            +"'fields.amount.min'='1','fields.amount.max'='100',"
            +"'fields.ts_ms.min'='1767225600000','fields.ts_ms.max'='1767225619999')");
        String group="SELECT user_id,SUM(amount) AS total FROM events GROUP BY user_id";
        String window="SELECT window_start,window_end,url,COUNT(*) AS cnt FROM TABLE("
            +"TUMBLE(TABLE events,DESCRIPTOR(et),INTERVAL '10' SECOND)) "
            +"GROUP BY window_start,window_end,url";
        t.executeSql("CREATE VIEW user_totals AS "+group);
        t.executeSql("CREATE VIEW url_window_counts AS "+window);
        plan(t,"group",group);
        plan(t,"tumble",window);
        plan(t,"hop","SELECT window_start,window_end,user_id,SUM(amount) FROM TABLE("
            +"HOP(TABLE events,DESCRIPTOR(et),INTERVAL '5' SECOND,INTERVAL '1' MINUTE)) "
            +"GROUP BY window_start,window_end,user_id");
        plan(t,"cumulate","SELECT window_start,window_end,user_id,SUM(amount) FROM TABLE("
            +"CUMULATE(TABLE events,DESCRIPTOR(et),INTERVAL '10' SECOND,INTERVAL '1' MINUTE)) "
            +"GROUP BY window_start,window_end,user_id");
        plan(t,"session","SELECT user_id,window_start,window_end,COUNT(*) FROM "
            +"SESSION(TABLE events PARTITION BY user_id,DESCRIPTOR(et),INTERVAL '10' SECOND) "
            +"GROUP BY user_id,window_start,window_end");
        plan(t,"over","SELECT user_id,amount,SUM(amount) OVER(PARTITION BY user_id ORDER BY et "
            +"RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW) FROM events");
        plan(t,"window-topn","SELECT * FROM (SELECT window_start,window_end,url,cnt,"
            +"ROW_NUMBER() OVER(PARTITION BY window_start,window_end ORDER BY cnt DESC,url ASC) rn "
            +"FROM url_window_counts) WHERE rn<=2");
        plan(t,"dedup","SELECT * FROM (SELECT *,ROW_NUMBER() OVER("
            +"PARTITION BY event_id ORDER BY et ASC) rn FROM events) WHERE rn=1");
        plan(t,"interval-join","SELECT a.event_id,b.event_id FROM events a JOIN events b "
            +"ON a.user_id=b.user_id AND b.et BETWEEN a.et AND a.et+INTERVAL '15' MINUTE");
        plan(t,"regular-join","SELECT a.event_id,b.event_id FROM events a LEFT JOIN events b "
            +"ON a.user_id=b.user_id");
        t.createTemporarySystemFunction("safe_ratio",CoreApiExamples.SafeRatio.class);
        t.createTemporarySystemFunction("avg_money",CoreApiExamples.AvgMoney.class);
        plan(t,"scalar-udf","SELECT safe_ratio(amount,user_id) FROM events");
        plan(t,"aggregate-udf","SELECT user_id,avg_money(amount) FROM events GROUP BY user_id");
        plan(t,"to-changelog","SELECT * FROM TO_CHANGELOG(input => TABLE user_totals)");
        t.executeSql("CREATE TABLE raw_changes(id BIGINT,operation STRING,name STRING) "
            +"WITH ('connector'='datagen','number-of-rows'='10')");
        plan(t,"from-changelog","SELECT * FROM FROM_CHANGELOG(input => TABLE raw_changes,"
            +"op => DESCRIPTOR(operation))");
        t.getConfig().set("table.exec.mini-batch.enabled","true");
        t.getConfig().set("table.exec.mini-batch.allow-latency","2 s");
        t.getConfig().set("table.exec.mini-batch.size","5000");
        t.getConfig().set("table.optimizer.agg-phase-strategy","TWO_PHASE");
        plan(t,"local-global",group);
        t.getConfig().set("table.optimizer.distinct-agg.split.enabled","true");
        t.getConfig().set("table.optimizer.distinct-agg.split.bucket-num","128");
        plan(t,"split-distinct","SELECT user_id,COUNT(DISTINCT event_id) FROM events GROUP BY user_id");
        plan(t,"distinct-filter","SELECT url,COUNT(DISTINCT user_id),COUNT(DISTINCT user_id) "
            +"FILTER(WHERE amount>50) FROM events GROUP BY url");
        System.out.println("PLAN TOTAL "+count);
    }
}

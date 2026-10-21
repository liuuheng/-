package notes.flink;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import notes.flink.Flink23Lab.*;

/** Compile-checked templates. External file/IO paths are not executed. */
public class CoreApiExamples {
    public static DataStream<Long> datagen(StreamExecutionEnvironment env) {
        DataGeneratorSource<Long> source=new DataGeneratorSource<>(i->i,1000L,Types.LONG);
        return env.fromSource(source,WatermarkStrategy.noWatermarks(),"finite-datagen");
    }
    public static DataStream<String> files(StreamExecutionEnvironment env,String inputUri) {
        FileSource<String> source=FileSource.forRecordStreamFormat(
            new TextLineInputFormat(),new Path(inputUri)).build();
        return env.fromSource(source,WatermarkStrategy.noWatermarks(),"input-files");
    }
    public static SingleOutputStreamOperator<Event> parse(DataStream<String> raw,OutputTag<String> dirty) {
        return raw.process(new ProcessFunction<String,Event>() {
            public void processElement(String line,Context ctx,Collector<Event> out) {
                try {
                    String[] f=line.split(",",-1);
                    if(f.length!=5||f[0].isBlank()) throw new IllegalArgumentException("invalid fields");
                    long ts=Long.parseLong(f[3]);
                    if(ts<0) throw new IllegalArgumentException("negative timestamp");
                    out.collect(new Event(f[0],f[1],f[2],ts,Long.parseLong(f[4])));
                } catch(IllegalArgumentException ex) {ctx.output(dirty,line+" | "+ex.getMessage());}
            }
        });
    }
    public static DataStream<String> join(DataStream<Event> left,DataStream<Event> right) {
        return left.keyBy(e->e.userId).intervalJoin(right.keyBy(e->e.userId))
            .between(Duration.ofMinutes(-5),Duration.ofMinutes(1))
            .process(new ProcessJoinFunction<Event,Event,String>() {
                public void processElement(Event a,Event b,Context ctx,Collector<String> out) {
                    out.collect(a.eventId+":"+b.eventId);
                }
            });
    }
    public static DataStream<WindowCount> late(DataStream<Event> timed,OutputTag<Event> tag) {
        return timed.keyBy(e->e.url)
            .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .allowedLateness(Duration.ofSeconds(5)).sideOutputLateData(tag)
            .aggregate(new CountAgg(),new AddWindow());
    }
    public static DataStream<String> rules(DataStream<Event> events,DataStream<Tuple2<String,Long>> rules) {
        MapStateDescriptor<String,Long> d=new MapStateDescriptor<>("rule-version-v1",Types.STRING,Types.LONG);
        return events.keyBy(e->e.userId).connect(rules.broadcast(d))
            .process(new KeyedBroadcastProcessFunction<String,Event,Tuple2<String,Long>,String>() {
                public void processBroadcastElement(Tuple2<String,Long> r,Context ctx,Collector<String> out) throws Exception {
                    Long old=ctx.getBroadcastState(d).get(r.f0);
                    if(old==null||r.f1>old)ctx.getBroadcastState(d).put(r.f0,r.f1);
                }
                public void processElement(Event e,ReadOnlyContext ctx,Collector<String> out) throws Exception {
                    Long v=ctx.getBroadcastState(d).get(e.url);
                    if(v!=null)out.collect(e.eventId+":rule="+v);
                }
            });
    }
    public static void sink(DataStream<String> lines,String outputUri) {
        FileSink<String> f=FileSink.forRowFormat(new Path(outputUri),new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15)).withInactivityInterval(Duration.ofMinutes(5))
                .withMaxPartSize(MemorySize.ofMebiBytes(128)).build()).build();
        lines.sinkTo(f).uid("result-files-v1");
    }
    public static void async(DataStream<Event> input,AsyncFunction<Event,String> lookup) {
        AsyncDataStream.unorderedWait(input,lookup,2,TimeUnit.SECONDS,100);
    }
    public static class SafeRatio extends ScalarFunction {
        public Double eval(Long n,Long d) {return n==null||d==null||d==0?null:(double)n/d;}
    }
    public static class AvgAcc {public long sum;public long count;}
    public static class AvgMoney extends org.apache.flink.table.functions.AggregateFunction<Double,AvgAcc> {
        public AvgAcc createAccumulator(){return new AvgAcc();}
        public Double getValue(AvgAcc a){return a.count==0?null:(double)a.sum/a.count;}
        public void accumulate(AvgAcc a,Long v){if(v!=null){a.sum+=v;a.count++;}}
        public void retract(AvgAcc a,Long v){if(v!=null){a.sum-=v;a.count--;}}
        public void merge(AvgAcc a,Iterable<AvgAcc> others){for(AvgAcc b:others){a.sum+=b.sum;a.count+=b.count;}}
    }
}

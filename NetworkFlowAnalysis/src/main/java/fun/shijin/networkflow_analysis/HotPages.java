package fun.shijin.networkflow_analysis;

import fun.shijin.networkflow_analysis.beans.ApacheLogEvent;
import fun.shijin.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Author Jiaman
 * @Date 2021/4/12 16:38
 * @Desc
 */

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop111", 7777);

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                });

        dataStream.print("data");

        // 定义一个侧输出流
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.print("agg");

        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口的count数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();

        env.execute("Hot pages job");
                
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-view-count-map-state", String.class, Long.class));
        }


        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L){
                pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

            // 将排名信息格式化string，方便打印输出
            StringBuffer resultBuilder = new StringBuffer();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");


            // 遍历列表，取top n 输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("No ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===================================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}

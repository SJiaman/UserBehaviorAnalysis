package fun.shijin.networkflow_analysis;

import fun.shijin.networkflow_analysis.beans.PageViewCount;
import fun.shijin.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


import javax.swing.*;
import java.net.URL;
import java.util.Random;

/**
 * @Author Jiaman
 * @Date 2021/4/13 10:31
 * @Desc  PV统计
 */

public class PageView {
    public static void main(String[] args) throws Exception{
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 3.数据转换
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });

        // 4.分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤pv行为
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1)) //开一小时窗口
                .sum(1);// 统计count值

//        pvResultStream0.print();


        // 4.1改进并行任务，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PVCountAgg(), new PVCountResult());



        // 4.2将分区数据汇总
        SingleOutputStreamOperator<PageViewCount> pvResultStream = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPVCount());

        pvResultStream.print();

        env.execute("page view job");

    }

    // 自定以聚合函数
    private static class PVCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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

    private static class PVCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(integer.toString(), timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    // 自定义处理函数
    private static class TotalPVCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            totalCountState.update( totalCountState.value() + value.getCount() );
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            // 清空状态
            totalCountState.clear();
        }

    }
}

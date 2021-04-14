package fun.shijin.networkflow_analysis;


import fun.shijin.networkflow_analysis.beans.PageViewCount;
import fun.shijin.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;

/**
 * @Author Jiaman
 * @Date 2021/4/14 11:02
 * @Desc UV 统计
 */

public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 3.数据转换
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream
                .map(data -> {
                    String[] fields = data.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<PageViewCount> UVCountResultStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UVCountResult());

        UVCountResultStream.print();
        env.execute("uv count");
    }

    // 实现自定义的全窗口函数
    private static class UVCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个Set结构，保存窗口中所有UserId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub: values)
                uidSet.add(ub.getUserId());
            out.collect(new PageViewCount("uv", window.getEnd(), (long)uidSet.size()));
        }
    }
}

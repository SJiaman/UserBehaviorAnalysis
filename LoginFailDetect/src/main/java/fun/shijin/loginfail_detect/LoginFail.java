package fun.shijin.loginfail_detect;

import fun.shijin.loginfail_detect.beans.LoginEvent;
import fun.shijin.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;

/**
 * @Author Jiaman
 * @Date 2021/4/16 21:14
 * @Desc 恶意登录检测（一）
 */

public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventDataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventDataStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning());

        warningStream.print();

        env.execute("login fail detect job");

    }

    // 实现自定义KeyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 定义状态，保存2s内所有登录失败的事件
        ListState<LoginEvent> loginEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list",
                    LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 如果登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 如果已经有登录失败事件，继续判断时间是否在2s之内
                    // 获取已有的失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        // 如果在2秒内，输出报警
                        out.collect(new LoginFailWarning(value.getUserId(), firstFailEvent.getTimestamp(), value.getTimestamp(),
                                "login fail 2 times in 2s"));
                    }
                    // 不管报不报警，都处理完毕，直接更新状态
                    loginEventListState.clear();
                    loginEventListState.add(value);
                }else {
                    // 如果没有登录失败，直接将事件存入ListState
                    loginEventListState.add(value);
                }
            }else {
                // 登录成功，直接清空状态
                loginEventListState.clear();
            }
        }
    }
}

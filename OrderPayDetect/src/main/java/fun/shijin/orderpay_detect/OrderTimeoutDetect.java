package fun.shijin.orderpay_detect;

import fun.shijin.orderpay_detect.beans.OrderEvent;
import fun.shijin.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @Author Jiaman
 * @Date 2021/4/19 18:44
 * @Desc
 */

public class OrderTimeoutDetect {
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout-tag"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderTimeoutDetect.class.getResource("/OrderLog.csv");
        DataStreamSource<String> dataStream = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<OrderEvent> orderEventStream = dataStream
                .map(data -> {
                    String[] fields = data.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order pay detect");
    }

    // 自定义process方法
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        // 定义状态保存之前订单是否有create，pay的事件
        ValueState<Boolean> isPayState;
        ValueState<Boolean> isCreateState;
        // 定义状态，保存定时器事件戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            // 获取状态
            Boolean isPayed = isCreateState.value();
            Boolean isCreated = isCreateState.value();
            Long timerTs = timerTsState.value();

            // 判断事件类型
            if ("create".equals(value.getEventType())) {
                // 判断是否支付
                if (isPayed) {
                    // 正常支付，输出正常结果
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));

                    isCreateState.clear();
                    isCreateState.clear();
                    timerTsState.clear();
                } else {
                    // 没有支付，注册15分钟的定时器，等待支付事件
                    Long ts = ((value.getTimestamp() + 15 * 60) * 1000L);
                    ctx.timerService().registerEventTimeTimer(ts);

                    // 更新状态
                    timerTsState.update(ts);
                    isCreateState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 判断是否有下单事件
                if (isCreated) {
                    // 判断是否超出15分钟
                    if (value.getTimestamp() * 1000L < timerTs) {
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    } else {
                        ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }

                    isCreateState.clear();
                    isPayState.clear();
                    timerTsState.clear();
                } else {
                    // 没有下单事件，乱序，注册一个定时器，等待下单事件
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);

                    timerTsState.update(value.getTimestamp() * 1000L);
                    isPayState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，有事件来
            if (isPayState.value()) {
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found create log"));
            } else {
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }

            isPayState.clear();
            isCreateState.clear();
            timerTsState.clear();
        }
    }
}

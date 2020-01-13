package fun.cosmozhu.session19.main;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session19.datasource.ExchangeRateDataSource;
import fun.cosmozhu.session19.datasource.OrderDataSource;
import fun.cosmozhu.session19.pojo.CurrencyType;
import fun.cosmozhu.session19.pojo.ExchangeRateInfo;
import fun.cosmozhu.session19.pojo.OrderInfo;

/**
 * split - 将混合在一起的字符串订单流和汇率流，分离成2个字符串流
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		//CNY -> USD 汇率流
		SingleOutputStreamOperator<ExchangeRateInfo> cnyToUsd = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6),"USD-CNY")
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ExchangeRateInfo>(Time.milliseconds(100)) {
					@Override
					public long extractTimestamp(ExchangeRateInfo element) {
						return element.getTimeStamp().getTime();
					}
				});
		
		//订单流
		SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.milliseconds(100)) {
					@Override
					public long extractTimestamp(OrderInfo element) {
						return element.getTimeStamp().getTime();
					}
				});
		
	
		
		SplitStream<String> split = cnyToUsd.connect(orderDs)
		.map(new CoMapFunction<ExchangeRateInfo, OrderInfo, String>() {
			@Override
			public String map1(ExchangeRateInfo value) throws Exception {
				return value.toString();
			}
			@Override
			public String map2(OrderInfo value) throws Exception {
				return value.toString();
			}
		})
		//将混合在一起的订单流和汇率流分离出来
		.split(new OutputSelector<String>() {
			@Override
			public Iterable<String> select(String value) {
				List<String> output = new ArrayList<String>();
				if(value.startsWith("OrderInfo")) {
					output.add("order");
				}else {
					output.add("exchange");
				}
				return output;
			}
		});

		//选择订单流
		split.select("order").addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				LOG.info("order::"+value);
			}
		});
		
		//选择汇率流
		split.select("exchange").addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				LOG.info("exchange::"+value);
			}
		});
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

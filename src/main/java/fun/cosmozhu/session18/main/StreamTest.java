package fun.cosmozhu.session18.main;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session18.datasource.ExchangeRateDataSource;
import fun.cosmozhu.session18.datasource.OrderDataSource;
import fun.cosmozhu.session18.pojo.CurrencyType;
import fun.cosmozhu.session18.pojo.ExchangeRateInfo;
import fun.cosmozhu.session18.pojo.OrderInfo;

/**
 * connect - 将订单流和汇率流合为一个流，并转换为字符串流
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
		
	
		
		cnyToUsd.connect(orderDs)
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
		.print();
		
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

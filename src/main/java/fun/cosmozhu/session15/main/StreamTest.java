package fun.cosmozhu.session15.main;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session15.datasource.ExchangeRateDataSource;
import fun.cosmozhu.session15.datasource.OrderDataSource;
import fun.cosmozhu.session15.pojo.CurrencyType;
import fun.cosmozhu.session15.pojo.ExchangeRateInfo;
import fun.cosmozhu.session15.pojo.OrderInfo;

/**
 * join - 流关联，关联订单流和汇率流，实时计算当前订单的外汇金额
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		//CNY -> USD 汇率流
		SingleOutputStreamOperator<ExchangeRateInfo> usdToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6),"USD-CNY")
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
		
		//订单流inner join 汇率流
		orderDs.join(usdToCny)
		.where(new KeySelector<OrderInfo, CurrencyType>() {
			private static final long serialVersionUID = 1L;
			@Override
			public CurrencyType getKey(OrderInfo value) throws Exception {
				return value.getCurrencyType();
			}
		})
		.equalTo(new KeySelector<ExchangeRateInfo, CurrencyType>() {
			private static final long serialVersionUID = 1L;
			@Override
			public CurrencyType getKey(ExchangeRateInfo value) throws Exception {
				return value.getFrom();
			}
		})
		//转换20s窗口期内的订单金额为美元
		.window(TumblingEventTimeWindows.of(Time.seconds(10)))
		//将订单金额人民币转换为美元
		.apply(new JoinFunction<OrderInfo, ExchangeRateInfo, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String join(OrderInfo first, ExchangeRateInfo second) throws Exception {
				return "$"+first.getTotalAmt().divide(second.getCoefficient(), 2,BigDecimal.ROUND_HALF_UP).toPlainString();
			}
		})
		.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

package fun.cosmozhu.session17.main;

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session17.datasource.ExchangeRateDataSource;
import fun.cosmozhu.session17.datasource.OrderDataSource;
import fun.cosmozhu.session17.pojo.CurrencyType;
import fun.cosmozhu.session17.pojo.ExchangeRateInfo;
import fun.cosmozhu.session17.pojo.OrderInfo;

/**
 * coGroup - 类似于left join
 * 本例中汇率流由两种汇率构成，将订单流与cny->usd流相关联。如果在窗口期内能关联到则输出转换为美元的金额，如果关联不到则输出人民币金额。
 * 可以看出join 实际是  coGroup的一种特殊情况
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
		//CNY -> EUR 汇率流
		SingleOutputStreamOperator<ExchangeRateInfo> cnyToEur = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.EUR, 8, 7),"USD-EUR")
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ExchangeRateInfo>(Time.milliseconds(100)) {
					@Override
					public long extractTimestamp(ExchangeRateInfo element) {
						return element.getTimeStamp().getTime();
					}
				});
		
		
		DataStream<ExchangeRateInfo> union = cnyToUsd.union(cnyToEur);
		
		//订单流
		SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.milliseconds(100)) {
					@Override
					public long extractTimestamp(OrderInfo element) {
						return element.getTimeStamp().getTime();
					}
				});
		
		//left join
		orderDs.coGroup(union)
		.where((KeySelector<OrderInfo, CurrencyType>) (order) -> order.getCurrencyType())
		.equalTo((exchange) -> exchange.getFrom())
		.window(TumblingEventTimeWindows.of(Time.seconds(10)))
		
		.apply(new CoGroupFunction<OrderInfo, ExchangeRateInfo, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void coGroup(Iterable<OrderInfo> first, Iterable<ExchangeRateInfo> second, Collector<String> out)
					throws Exception {
				
				Iterator<ExchangeRateInfo> exchangeIterator = second.iterator();
				ExchangeRateInfo cnyToUsdExchange = null;
				
				while(exchangeIterator.hasNext()) {
					ExchangeRateInfo exchange = exchangeIterator.next();
					if(exchange.getTo() == CurrencyType.USD) {
						cnyToUsdExchange = exchange;
						break;
					}
				}
				
				Iterator<OrderInfo> orderIterator = first.iterator();
				while(orderIterator.hasNext()) {
					OrderInfo order = orderIterator.next();
					if(cnyToUsdExchange != null) {
						out.collect("$"+order.getTotalAmt().divide(cnyToUsdExchange.getCoefficient(),2,BigDecimal.ROUND_HALF_UP).toPlainString());
					}else {
						out.collect("￥"+order.getTotalAmt().toPlainString());

					}
				}
			}
			
		})
		.print();

		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

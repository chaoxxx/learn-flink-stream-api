package fun.cosmozhu.session24;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session22.pojo.OrderInfo;
import fun.cosmozhu.session22.util.SerializerUtil;
/**
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	final static String defaultTopic = "orderlist";
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.4.53:9094");
		properties.setProperty("transaction.timeout.ms", "3600000");
		FlinkKafkaConsumer<OrderInfo> fc = new FlinkKafkaConsumer<OrderInfo>(defaultTopic,new DeserializationSchema<OrderInfo>() {
			private static final long serialVersionUID = 8709669654807220414L;
			@Override
			public TypeInformation<OrderInfo> getProducedType() {
				return TypeInformation.of(OrderInfo.class);
			}
			@Override
			public OrderInfo deserialize(byte[] message) throws IOException {
				return SerializerUtil.deserialize(message,OrderInfo.class);
			}
			@Override
			public boolean isEndOfStream(OrderInfo nextElement) {
				return false;
			}
		},properties);
		DataStreamSource<OrderInfo> orderInfo = env.addSource(fc);
		DataStream<OrderInfo> filter = orderInfo.filter(v -> v.getTotalAmt().compareTo(BigDecimal.ZERO)!= 0);
		
		filter.print();
		filter.timeWindowAll(Time.minutes(1), Time.seconds(10))
		.aggregate(new AggregateFunction<OrderInfo, HashMap<String,BigDecimal>, BigDecimal>() {
			private static final long serialVersionUID = -1769478830993581049L;
			@Override
			public HashMap<String, BigDecimal> createAccumulator() {
				LOG.info("初始化 accumulator！");
				return new HashMap<String, BigDecimal>();
			}
			@Override
			public HashMap<String, BigDecimal> add(OrderInfo value, HashMap<String, BigDecimal> accumulator) {
				BigDecimal oldNum = accumulator.getOrDefault("num", BigDecimal.ZERO);
				LOG.info("当前accumulator的num:{}",oldNum.toPlainString());
				accumulator.put("num", oldNum.add(BigDecimal.ONE));
				BigDecimal oldTotalAmt = accumulator.getOrDefault("totalAmt", BigDecimal.ZERO);
				LOG.info("当前accumulator的oldTotalAmt:{}",oldTotalAmt.toPlainString());
				accumulator.put("totalAmt", oldNum.add(value.getTotalAmt()));
				return accumulator;
			}

			@Override
			public BigDecimal getResult(HashMap<String, BigDecimal> accumulator) {
				BigDecimal oldNum = accumulator.getOrDefault("num", BigDecimal.ZERO);
				BigDecimal oldTotalAmt = accumulator.getOrDefault("totalAmt", BigDecimal.ZERO);
				LOG.info("getResult:获得的oldNum{}",oldNum.toPlainString());
				LOG.info("getResult:获得的oldTotalAmt{}",oldTotalAmt.toPlainString());
				return oldTotalAmt.divide(oldNum,2,BigDecimal.ROUND_HALF_UP);
			}

			@Override
			public HashMap<String, BigDecimal> merge(HashMap<String, BigDecimal> a, HashMap<String, BigDecimal> b) {
				LOG.info("执行merge！！！");
				return null;
			}
		}).print();
		
		
		env.execute("process order");
	}
}	

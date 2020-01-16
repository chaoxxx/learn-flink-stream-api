package fun.cosmozhu.session23;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session22.pojo.OrderInfo;
import fun.cosmozhu.session22.util.SerializerUtil;
/**
 * 读取并打印kafka队列的消息
 * 这里发现了一个问题 ，在win7开发机里边，链接linux中docker的kafka报错 No entry found for connection 1001
 * 这个解决方法再win7中的hosts文件添加 containerid 的映射
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
		SingleOutputStreamOperator<OrderInfo> filter = orderInfo.filter(v -> v.getTotalAmt().compareTo(BigDecimal.ZERO)!= 0);
		filter.print();
		env.execute("process order");
	}
}	

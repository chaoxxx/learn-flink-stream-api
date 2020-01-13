package fun.cosmozhu.session22;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session22.datasource.OrderDataSource;
import fun.cosmozhu.session22.pojo.OrderInfo;
import fun.cosmozhu.session22.util.SerializerUtil;
/**
 * 将生成的订单放入kafka
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
		DataStreamSource<OrderInfo> ds = env.addSource(new OrderDataSource());
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.4.53:9092");
		properties.setProperty("transaction.timeout.ms", "3600000");
		final FlinkKafkaProducer<OrderInfo> producer = new FlinkKafkaProducer<OrderInfo>(defaultTopic,new KafkaSerializationSchema<OrderInfo>() {
			private static final long serialVersionUID = -3639074352174660801L;
			@Override
			public ProducerRecord<byte[], byte[]> serialize(OrderInfo element, Long timestamp) {
				ProducerRecord<byte[], byte[]> pr = null;
				pr = new ProducerRecord<byte[], byte[]>(defaultTopic,SerializerUtil.serialize(element));
				return pr;
			}
		},properties,Semantic.EXACTLY_ONCE);
		ds.addSink(producer);
		env.execute("add order");
	}
}	

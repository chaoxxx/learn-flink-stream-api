package fun.cosmozhu.session2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String[] words = {"apple","orange","banana","watermelon"};
		//创建DataSource
		//根据给定的元素创建一个数据流，元素必须是相同的类型，比如全部为String，或者全部为int。
		DataStreamSource<String> ds = env.fromElements(words);
		//Transformations
		//对DataStream应用一个Map转换。对DataStream中的每一个元素都会调用MapFunction接口的具体实现类。map方法仅仅只返回一个元素，这个flatMap不同
		DataStream<String> map = ds.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value.toUpperCase();
			}
		});
		//sinks打印出信息
		//给DataStream添加一个Sinks
		map.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				LOG.info(value);
			}
		});
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

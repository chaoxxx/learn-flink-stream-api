package fun.cosmozhu.session2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String[] words = {"apple","orange","banana","watermelon"};
		//创建DataSource
		DataStreamSource<String> ds = env.fromElements(words);
		//Transformations
		DataStream<String> map = ds.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value.toUpperCase();
			}
		});
		//sinks打印出信息
		map.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				LOG.info(value);
			}
		});
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

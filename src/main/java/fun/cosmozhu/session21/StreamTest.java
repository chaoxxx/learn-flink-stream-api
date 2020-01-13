package fun.cosmozhu.session21;

import java.math.BigDecimal;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * project - 定制流字段，只能用于tuple类型
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 DataStreamSource<Tuple5<String, GoodsType, BigDecimal, CurrencyType, Integer>> addSource = env.addSource(new GoodsDataSource());
		SingleOutputStreamOperator<Tuple> project = addSource.project(0,2,4);
		project.print();
		env.execute("Flink Streaming Java API Skeleton");
	}
}

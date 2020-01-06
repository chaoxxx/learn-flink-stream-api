package fun.cosmozhu.session5;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建一个简单的订单数据流数据结构为{商品名称,商品数量}，
 * 实时统计每种商品的商品数量
 * 设计函数keyBy、sum、print
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	private static final String[] TYPE = { "苹果", "梨", "西瓜", "葡萄", "火龙果" };

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//添加自定义数据源,每秒发出一笔订单信息{商品名称,商品数量}
		DataStreamSource<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			private volatile boolean isRunning = true;
			private final Random random = new Random();
			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				while (isRunning) {
					TimeUnit.SECONDS.sleep(1);
					ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], 1));
				}
			}
			@Override
			public void cancel() {
				isRunning = false;
			}

		}, "order-info");
		
		orderSource
		//将订单流按Tuple的第一个字段分区--商品名称
		.keyBy(0)
		//累加订单流Tuple的第二个字段--商品数量
		.sum(1)
		//将流中的每个元素按标准输出打印
		.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

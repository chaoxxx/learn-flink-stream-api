package fun.cosmozhu.session13;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * min与minby的区别：min只返回最小数值而不带订单名，minby既返回最小数值又返回此数值的订单名
 * max与maxby其相似
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	private static final String[] TYPE = { "苹果", "梨", "西瓜", "葡萄", "火龙果" };

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 添加自定义数据源,每秒发出一笔订单信息{商品名称,商品数量}
		DataStreamSource<Tuple2<String, Integer>> orderSource = env
				.addSource(new SourceFunction<Tuple2<String, Integer>>() {
					private volatile boolean isRunning = true;
					private final Random random = new Random();

					@Override
					public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
						while (isRunning) {
							TimeUnit.SECONDS.sleep(1);
							Tuple2<String, Integer> t = Tuple2.of(TYPE[random.nextInt(TYPE.length)], random.nextInt(1000));
							LOG.info("提交数据："+t);
							ctx.collect(t);
						}
					}
					@Override
					public void cancel() {
						isRunning = false;
					}

				}, "order-info");
		
		orderSource
		.timeWindowAll(Time.minutes(1),Time.seconds(10))
//		.min(1)
		.minBy(1)
		.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

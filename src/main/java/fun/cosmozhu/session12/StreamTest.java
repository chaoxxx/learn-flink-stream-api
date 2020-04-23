package fun.cosmozhu.session12;

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
 * apply 函数的应用
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
							ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], 1));
						}
					}
					@Override
					public void cancel() {
						isRunning = false;
					}

				}, "order-info");
		orderSource.
				// 	未使用keyby分区流的时间窗口
				timeWindowAll(Time.seconds(10))
				// apply 函数是对窗口内的数据做处理的核心方法。这是对10s窗口中的所有元素做过滤，只输出商品名称为苹果的订单
				.apply(new AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
					@Override
					public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values,
							Collector<String> out) throws Exception {
						values.forEach(v -> {
							if(v.f0.contentEquals("苹果")) {
								out.collect(v.f0 + ":" + v.f1);
							}
						});
					}
				}).print();

		env.execute("Flink Streaming Java API Skeleton");
	}
}

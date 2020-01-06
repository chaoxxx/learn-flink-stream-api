package fun.cosmozhu.session9;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * window-Sliding时间窗口：每10s统计一次最近1min内的订单数量
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
		//这里只为将DataStream → KeyedStream,用空字符串做分区键。所有数据为相同分区
		orderSource.keyBy(new KeySelector<Tuple2<String,Integer>, String>(){
			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return "";
			}
			
		})
		//每10s统计一次最近1min内的订单数量
		.timeWindow(Time.minutes(1), Time.seconds(10))
		//这里用HashMap做暂存器
		.fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String,Integer>, Map<String, Integer>>() {
			@Override
			public Map fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
				accumulator.put(value.f0, (Integer)accumulator.getOrDefault(value.f0, 0)+value.f1);
				return accumulator;
			}
		})
		.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

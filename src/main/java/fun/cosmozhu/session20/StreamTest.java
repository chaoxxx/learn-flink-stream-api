package fun.cosmozhu.session20;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * iterate - 利用iterate的反馈机制制作fib，在到达指定fib数后，停止反馈
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		List<Long> initial = new ArrayList<Long>();
		initial.add(1L);
		DataStreamSource<Long> ds = env.fromCollection(initial);
		IterativeStream<Long> iterate = ds.iterate(1000);
		
		SingleOutputStreamOperator<HashMap<String, Long>> fold = iterate.keyBy(key -> "").fold(new HashMap<String,Long>(), new FoldFunction<Long, HashMap<String,Long>>() {
			@Override
			public HashMap<String, Long> fold(HashMap<String,Long> accumulator, Long value) throws Exception {
				Long old = accumulator.getOrDefault("pre", 0L);
				accumulator.put("pre", value);
				accumulator.put("next", old+value);
				LOG.info("now:"+value);
				return accumulator;
			}
		});
		
		//设置fib数最大上限
		SplitStream<Long> split = fold.map(v -> v.get("next")).setParallelism(1).split(new OutputSelector<Long>() {
			@Override
			public Iterable<String> select(Long value) {
				List<String> outPut = new ArrayList<String>(2);
				if(value < 4052739537881L) {
					outPut.add("feedback");
				}else {
					outPut.add("out");
				}
				return outPut;
			}
		});
		
		DataStream<Long> closeWith = iterate.closeWith(split.select("feedback"));
		DataStream<Long> out = split.select("out");
		out.print();
		env.execute("Flink Streaming Java API Skeleton");
	}
}

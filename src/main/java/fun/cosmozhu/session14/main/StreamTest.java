package fun.cosmozhu.session14.main;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fun.cosmozhu.session14.datasource.ExchangeRateDataSource;
import fun.cosmozhu.session14.pojo.CurrencyType;
import fun.cosmozhu.session14.pojo.ExchangeRateInfo;

/**
 * union 流合并，将多个流合并成一个流
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class StreamTest {
	private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//USD -> CNY 汇率流
		DataStreamSource<ExchangeRateInfo> usdToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.USD, CurrencyType.CNY, 7, 6),"USD-CNY");
		//EUR -> CNY 汇率流
		DataStreamSource<ExchangeRateInfo> eurToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.EUR, CurrencyType.CNY, 8, 7),"EUR-CNY");
		//AUD -> CNY 汇率流
		DataStreamSource<ExchangeRateInfo> audToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.AUD, CurrencyType.CNY, 5, 4),"AUD-CNY");
		//三个流合并为一个流
		DataStream<ExchangeRateInfo> allExchangeRate = usdToCny.union(eurToCny).union(audToCny);
		//将流标准输出
		allExchangeRate.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}

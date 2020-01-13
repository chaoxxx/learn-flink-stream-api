package fun.cosmozhu.session14.datasource;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import fun.cosmozhu.session14.pojo.CurrencyType;
import fun.cosmozhu.session14.pojo.ExchangeRateInfo;
/**
 * 随机产生汇率数据
 * @author cosmozhu
 * @mail zhuchao1103@gmail.com
 * @site http://www.cosmozhu.fun
 */
public class ExchangeRateDataSource implements SourceFunction<ExchangeRateInfo> {
	private static final long serialVersionUID = 4836546999687545904L;
	private volatile boolean isRunning = true;
	private CurrencyType from;
	private CurrencyType to;
	private int max = 0;
	private int min = 0;

	public ExchangeRateDataSource(CurrencyType from, CurrencyType to, int max, int min) {
		this.from = from;
		this.to = to;
		this.max = max;
		this.min = min;
	}

	@Override
	public void run(SourceContext<ExchangeRateInfo> ctx) throws Exception {
		while (isRunning) {
			TimeUnit.SECONDS.sleep(10);
			
			ExchangeRateInfo exchangeRateInfo = new ExchangeRateInfo(from, to,
					new BigDecimal(min + ((max - min) * new Random().nextFloat())).setScale(2, BigDecimal.ROUND_HALF_UP));
			
			ctx.collectWithTimestamp(exchangeRateInfo, System.currentTimeMillis());
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}

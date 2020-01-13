package fun.cosmozhu.session14.pojo;

import java.math.BigDecimal;

public class ExchangeRateInfo {
	private CurrencyType from;
	private CurrencyType to;
	private BigDecimal coefficient;
	
	public ExchangeRateInfo(CurrencyType from, CurrencyType to, BigDecimal coefficient) {
		this.from = from;
		this.to = to;
		this.coefficient = coefficient;
	}
	public CurrencyType getFrom() {
		return from;
	}
	public void setFrom(CurrencyType from) {
		this.from = from;
	}
	public CurrencyType getTo() {
		return to;
	}
	public void setTo(CurrencyType to) {
		this.to = to;
	}
	public BigDecimal getCoefficient() {
		return coefficient;
	}
	public void setCoefficient(BigDecimal coefficient) {
		this.coefficient = coefficient;
	}
	@Override
	public String toString() {
		return "ExchangeRateInfo [from=" + from + ", to=" + to + ", coefficient=" + coefficient + "]";
	}
}

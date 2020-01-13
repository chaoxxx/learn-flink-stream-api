package fun.cosmozhu.session22.pojo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class OrderInfo implements Serializable{
	private static final long serialVersionUID = 8951017406210476468L;
	private String orderNo;
	private Date timeStamp;
	private BigDecimal totalAmt;
	private List<Goods> goods;
	private CurrencyType currencyType;
	public String getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(String orderNo) {
		this.orderNo = orderNo;
	}
	public Date getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}
	public BigDecimal getTotalAmt() {
		return totalAmt;
	}
	public void setTotalAmt(BigDecimal totalAmt) {
		this.totalAmt = totalAmt;
	}
	public List<Goods> getGoods() {
		return goods;
	}
	public void setGoods(List<Goods> goods) {
		this.goods = goods;
	}
	public CurrencyType getCurrencyType() {
		return currencyType;
	}
	public void setCurrencyType(CurrencyType currencyType) {
		this.currencyType = currencyType;
	}
	@Override
	public String toString() {
		return "OrderInfo [orderNo=" + orderNo + ", timeStamp=" + timeStamp + ", totalAmt=" + totalAmt + ", goods="
				+ goods + ", currencyType=" + currencyType + "]";
	}
}

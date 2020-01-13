package fun.cosmozhu.session21;

import java.math.BigDecimal;

public class Goods {
	private String goodsNo;
	private GoodsType goodsType;
	private BigDecimal unitPrice;
	private CurrencyType currencyType;
	private int num;
	public Goods(String goodsNo, GoodsType goodsType, BigDecimal unitPrice, CurrencyType currencyType, int num) {
		this.goodsNo = goodsNo;
		this.goodsType = goodsType;
		this.unitPrice = unitPrice;
		this.currencyType = currencyType;
		this.num = num;
	}
	public String getGoodsNo() {
		return goodsNo;
	}
	public void setGoodsNo(String goodsNo) {
		this.goodsNo = goodsNo;
	}
	public GoodsType getGoodsType() {
		return goodsType;
	}
	public void setGoodsType(GoodsType goodsType) {
		this.goodsType = goodsType;
	}
	public BigDecimal getUnitPrice() {
		return unitPrice;
	}
	public void setUnitPrice(BigDecimal unitPrice) {
		this.unitPrice = unitPrice;
	}
	public CurrencyType getCurrencyType() {
		return currencyType;
	}
	public void setCurrencyType(CurrencyType currencyType) {
		this.currencyType = currencyType;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	@Override
	public String toString() {
		return "Goods [goodsNo=" + goodsNo + ", goodsType=" + goodsType + ", unitPrice=" + unitPrice + ", currencyType="
				+ currencyType + ", num=" + num + "]";
	}
}

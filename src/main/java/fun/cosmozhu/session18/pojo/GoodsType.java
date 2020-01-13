package fun.cosmozhu.session18.pojo;

public enum GoodsType {
	apple("苹果"),pear("梨"),grape("葡萄"),watermellon("西瓜"),pitaya("火龙果");
	
	private final String name;
	
	private GoodsType(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}

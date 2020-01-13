package fun.cosmozhu.session22.util;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;

import fun.cosmozhu.session22.pojo.CurrencyType;
import fun.cosmozhu.session22.pojo.GoodsType;

@SuppressWarnings("unchecked")
public class SerializerUtil {
	private static SerializeConfig config ;
	static {
		SerializeConfig config = new SerializeConfig();
	    config.configEnumAsJavaBean(GoodsType.class,CurrencyType.class);
	}
	private final static Logger LOG = LoggerFactory.getLogger(SerializerUtil.class);
	//序列化
	public static <T extends Serializable> byte[] serialize(T obj){
		 return JSON.toJSON(obj).toString().getBytes();
	}
	//反序列化
	@SuppressWarnings("unchecked")
	public static <T extends Serializable> T deserialize(byte[] bytes,Class<T> clazz){
		return JSONObject.parseObject(bytes, clazz);
	}
}

package org.myhbase.domain;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.myhbase.annotation.HBaseColumn;
import org.myhbase.annotation.HBaseRowKey;
import org.myhbase.annotation.HBaseTimestamp;

/**
 * 
 * @author wangyankai
 * 2017年2月10日
 */
public class ReflectCache {
	
	private static final ConcurrentHashMap<String, ReflectClass> classMap = new ConcurrentHashMap<String, ReflectClass>();
	
	public static ReflectClass get(String name) {
		return classMap.get(name);
	}
	
	public static <T> ReflectClass putIfAbsent(Class<T> clazz) {
		ReflectClass reflectClass = get(clazz.getName());
		if (reflectClass == null) {
			reflectClass = put(clazz);
		}
		return reflectClass;
	}
	
	public static <T> ReflectClass put(Class<T> clazz) {
		ReflectClass reflectClass = reflectClass(clazz);
		classMap.put(clazz.getName(), reflectClass);
		return reflectClass;
	}
	
	private static <T> ReflectClass reflectClass(Class<T> clazz) {
		
		//字段映射
    	ReflectClass rc = new ReflectClass();
    	rc.setName(clazz.getName());
    	for ( Field f : clazz.getDeclaredFields()) {
			ReflectField rf = new ReflectField();
			rf.setField(f);
			rf.setHBaseColumn(f.getAnnotation(HBaseColumn.class));
			rc.getFields().add(rf);
		}
    	
    	int flag = 0;
    	for ( Method m : clazz.getDeclaredMethods()) {
    		
    		//生成rowKey方法
    		if(rc.getRowKeyMethod() == null) {
    			HBaseRowKey rk = m.getAnnotation(HBaseRowKey.class);
    			if(rk != null){
    				ReflectMethodRowKey rm = new ReflectMethodRowKey();
    				rm.setMethod(m);
    				rm.setHBaseRowKey(rk);
    				rc.setRowKeyMethod(rm);
    				++flag;
    			}
    		}
    		
    		//生成版本号
    		if(rc.getTimestampMethod() == null) {
    			HBaseTimestamp ts = m.getAnnotation(HBaseTimestamp.class);
        		if(ts != null) {
    				ReflectMethodTimestamp rm = new ReflectMethodTimestamp();
    				rm.setMethod(m);
    				rm.setHBaseTimestamp(ts);
    				rc.setTimestampMethod(rm);
    				++flag;
        		}
    		}
    		
    		if( flag == 2) {
    			break;
    		}
		}
    	
    	return rc;
	}

}

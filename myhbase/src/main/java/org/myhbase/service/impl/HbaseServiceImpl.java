package org.myhbase.service.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.myhbase.HbaseUtil;
import org.myhbase.annotation.HBaseTable;
import org.myhbase.domain.HbaseOutput;
import org.myhbase.domain.HbaseQueryInput;
import org.myhbase.domain.ReflectCache;
import org.myhbase.domain.ReflectClass;
import org.myhbase.domain.ReflectField;
import org.myhbase.service.HbaseService;

/**
 * 实现ORM
 * @author wangyankai
 * 2017年8月11日
 */
public class HbaseServiceImpl implements HbaseService {

    private final static Log logger = LogFactory.getLog(HbaseServiceImpl.class);

    @Override
    public <T> boolean insert(List<T> rows, Class<T> clazz) {

        boolean result = false;
        if (CollectionUtils.isEmpty(rows)) {
            logger.error("HbaseServiceImpl.insert: rows=" + rows);
            return false;
        }
        
        HBaseTable at = clazz.getAnnotation(HBaseTable.class);
        HTableInterface table = HbaseUtil.getTable(at.name());
        table.setAutoFlush(false);
        try {
        	
        	byte[] family = Bytes.toBytes(at.family());
        	ReflectClass reflectClass = ReflectCache.putIfAbsent(clazz);
        	Method rowKeyM = reflectClass.getRowKeyMethod().getMethod();
        	Method tsM = null;
        	if (reflectClass.getTimestampMethod() != null) {
        		tsM = reflectClass.getTimestampMethod().getMethod();
        	}
        	
            List<Put> puts = new ArrayList<Put>();
            for ( T row : rows ) {

            	String rowKey = (String)rowKeyM.invoke(row, null);
            	Long ts = null;
            	if(reflectClass.getTimestampMethod() != null){
            		ts = (Long)tsM.invoke(row, null);
            	}
            	
                Put put = new Put(Bytes.toBytes(rowKey));
                put.setDurability(Durability.ASYNC_WAL);
                for ( ReflectField rfd : reflectClass.getFields()) {
                	
                	Field fd = rfd.getField();
                	String column = null;
                	if (rfd.getHBaseColumn() == null) {
                		column = fd.getName();
                	} else if (rfd.getHBaseColumn().serialize()) {
                		column = rfd.getHBaseColumn().name();
                	} else {
                		continue;
                	}
					fd.setAccessible(true);
					Class<?> fdC = fd.getType();
					//基本类型
					if (fdC.isPrimitive()) {
						String fdn = fdC.getSimpleName();
						String valueStr = null;
			            if ("int".equals(fdn)) {
			            	valueStr = String.valueOf(fd.getInt(row));
			            	
			            } else if ("long".equals(fdn)) {
			            	valueStr = String.valueOf(fd.getLong(row));
			            	
			            }  else if ("boolean".equals(fdn)) {
			            	valueStr = String.valueOf(fd.getBoolean(row));
			            }
			            
			            if (valueStr != null) {
			            	if (ts == null) {
			            		put.add(family, Bytes.toBytes(column), Bytes.toBytes(valueStr));
			            	} else {
			            		put.add(family, Bytes.toBytes(column), ts, Bytes.toBytes(valueStr));
			            	}
			            }
					} else {
						if (ts == null) {
		            		put.add(family, Bytes.toBytes(column), Bytes.toBytes( fd.get(row).toString() ));
		            	} else {
		            		put.add(family, Bytes.toBytes(column), ts, Bytes.toBytes( fd.get(row).toString() ));
		            	}
					}
				}
                puts.add(put);
            }
            table.put(puts);
            table.flushCommits();
            result = true;
        } catch (Exception e) {
            result = false;
            logger.error("insert hbase:", e);
        } finally {
            /*try {
                table.close();
            } catch (IOException e) {
                logger.error("close table err:", e);
            }*/
        }
        return result;
    }
    
    @Override
    public <T> HbaseOutput<T> getScanner(HbaseQueryInput<T> input) {
		return getScanResult(input);
	}

    private <T> HbaseOutput<T> getScanResult(HbaseQueryInput<T> input) {
    	
    	HbaseOutput<T> output = new HbaseOutput<T>();
        // 为分页创建的封装类对象，下面有给出具体属性
        ResultScanner scanner = null;
        try {
            
        	Class<T> vrClass = input.getTableClass();
        	HBaseTable at = vrClass.getAnnotation(HBaseTable.class);
            HTableInterface table = HbaseUtil.getTable(at.name()); // 获取筛选对象
            scanner = table.getScanner(builderScan(input, at.family()));
           
			Map<String, Field> fieldMap = new HashMap<String, Field>();
			for (ReflectField rf : ReflectCache.putIfAbsent(vrClass).getFields()) {
				if (rf.getHBaseColumn() != null) {
					fieldMap.put(rf.getHBaseColumn().name(), rf.getField());
				} else {
					fieldMap.put(rf.getField().getName(), rf.getField());
				}
			}
			
            // 遍历扫描器对象， 并将需要查询出来的数据row key取出
            List<T> rows = new LinkedList<T>();
            Integer pageSize = input.getPageSize();
			// 不分页
			if (pageSize == null || pageSize == 0L) {
				for (Result result : scanner) {
					buildRow(rows, vrClass, result, fieldMap);
				}

			} else { // 分页

				Integer currentPage = input.getCurrentPage();
				Integer startLine = (currentPage - 1) * pageSize;
				Integer endLine = startLine + pageSize;
				int i = 0;
				for (Result result : scanner) {
					if (i >= startLine && i < endLine) {
						buildRow(rows, vrClass, result, fieldMap);
					} else {
						break;
					}
					i++;
				}

			}            
            
            output.setRows(rows);
            
        } catch (Exception e) {
            logger.error("getScanResult exception:", e);
            output.setCode("exception");
            output.setMsg("getScanResult exception");
            
        } finally {
            if (scanner != null)
                scanner.close();
        }
        
        return output;
    }

	private <T> void buildRow(List<T> rows, Class<T> vrClass, Result result, Map<String, Field> fieldMap)
			throws InstantiationException, IllegalAccessException, NoSuchFieldException {
		
		String rowKey = Bytes.toString(result.getRow());
		if (StringUtils.isNotBlank(rowKey)) {
			T row = vrClass.newInstance();
			Field rowKeyF = fieldMap.get("rowKey");
			if (rowKeyF != null) {
				rowKeyF.setAccessible(true);
				rowKeyF.set(row, rowKey);
			}
			
		    List<KeyValue> kvList = result.list();
		    if (CollectionUtils.isNotEmpty(kvList)) {
		    	long timestamp = 0;
		        for (KeyValue kv : kvList) {
		            
		            Field fc = fieldMap.get(Bytes.toString(kv.getQualifier()));
		            if (fc == null) {
		            	logger.error(" 没有匹配到字段 - "+ Bytes.toString(kv.getQualifier()));
		            	continue;
		            }
		            fc.setAccessible(true);
		            String value = Bytes.toString(kv.getValue());
		            
		            String fsn = fc.getType().getSimpleName();
		            if ("int".equals(fsn) || "Integer".equals(fsn)) {
		            	int valueInt = 0;
		            	try {
		            		valueInt = Integer.valueOf(value);
						} catch (Exception e) {
							valueInt = Bytes.toInt(kv.getValue());
						}
		            	fc.set(row, valueInt);
		            	
		            } else if ("String".equals(fsn)) {
		            	fc.set(row, value);
		            	
		            } else if ("boolean".equalsIgnoreCase(fsn)) {
		            	fc.set(row, Boolean.valueOf(value));
		            	
		            } else if ("long".equalsIgnoreCase(fsn)) {
		            	fc.set(row, Long.valueOf(value));
		            } else {
		            	logger.error(" 没有匹配到类型 - "+ fsn);
		            }
		            
		            timestamp = kv.getTimestamp();
		        }
		        
		        //版本号
	        	Field timestampF = fieldMap.get("timestamp");
	        	if (timestampF != null) {
	        		timestampF.setAccessible(true);
	        		timestampF.set(row, timestamp);
	        	}
	        	
		    }
		    rows.add(row);
		}
	}
     
    // 获取扫描器对象
    private static <T> Scan builderScan(HbaseQueryInput<T> input, String family) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(input.getStartRow()));
        scan.setStopRow(Bytes.toBytes(input.getStopRow()));
        
        if (input.getFilterList() != null) {
            scan.setFilter(input.getFilterList());
        }
        scan.addFamily(Bytes.toBytes(family));
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

        return scan;
    }

}

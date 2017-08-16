package org.myhbase.domain;

import org.apache.hadoop.hbase.filter.FilterList;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author wangyankai
 * 2017年8月16日
 * @param <T>
 */
@ToString
public class HbaseQueryInput<T> {

	@Getter @Setter private String rowKey;
	@Getter @Setter private String startRow;
	@Getter @Setter private String stopRow;
	@Getter @Setter private Integer currentPage;
	@Getter @Setter private Integer pageSize;
	@Getter @Setter private Class<T> tableClass;
	@Getter @Setter private FilterList filterList;
	
}

package org.myhbase.domain;

import org.apache.hadoop.hbase.filter.FilterList;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class HbaseQueryInput<T> {

	@Getter @Setter private String rowKey;
	@Getter @Setter private String startRow;
	@Getter @Setter private String stopRow;
	@Getter @Setter private Integer currentPage;
	@Getter @Setter private Integer pageSize;
	@Getter @Setter private String tableName;
	@Getter @Setter private Class<T> tableClass;
	@Getter @Setter private String family;
	@Getter @Setter private FilterList filterList;
	
}

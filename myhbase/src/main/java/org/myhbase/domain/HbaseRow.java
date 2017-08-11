package org.myhbase.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class HbaseRow {

	@Getter @Setter private String tableName;
	@Getter @Setter private String rowKey;
	@Getter @Setter private String family;
	@Getter @Setter private String column;
	@Getter @Setter private String value;
	@Getter @Setter private long timestamp;
	
}

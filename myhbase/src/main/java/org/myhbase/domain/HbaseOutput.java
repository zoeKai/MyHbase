package org.myhbase.domain;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class HbaseOutput<T> {

	@Getter @Setter private String code;
	@Getter @Setter private String msg;
	@Getter @Setter private List<T> rows;
	
}

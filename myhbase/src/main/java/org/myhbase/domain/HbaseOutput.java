package org.myhbase.domain;

import java.util.List;

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
public class HbaseOutput<T> {

	@Getter @Setter private String code;
	@Getter @Setter private String msg;
	@Getter @Setter private List<T> rows;
	
}

package org.myhbase.service;

import java.util.List;

import org.myhbase.domain.HbaseOutput;
import org.myhbase.domain.HbaseQueryInput;

/**
 * 通用API
 * @author wangyankai
 * 2017年8月11日
 */
public interface HbaseService {

    /**
     * 查询
     * @param input
     * @return
     */
	<T> HbaseOutput<T> getScanner(HbaseQueryInput<T> input);

	/**
	 * 批量插入
	 * @param rows
	 * @param clazz
	 * @return
	 */
	<T> boolean insert(List<T> rows, Class<T> clazz);

}

package com.adanac.framework.dac.datasource;

import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

/**
 * shard注入接口
 * @author adanac
 * @version 1.0
 */
public interface ShardRegister {
	/**
	 * 功能描述：获取数据源
	 * 输入参数：身法字符串
	 */
	DataSource getDataSource(String identity);

	/**
	 * 功能描述：获取所有数据源
	 */
	Map<String, DataSource> getDataSources();

	/**
	 * 功能描述：获取所有shard分片
	 */
	Set<Shard> getShards();
}

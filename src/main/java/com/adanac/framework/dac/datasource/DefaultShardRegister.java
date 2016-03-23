package com.adanac.framework.dac.datasource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

/**
 * 默认shard注入类
 * @author adanac
 * @version 1.0
 */
public class DefaultShardRegister implements ShardRegister, InitializingBean {
	private Set<Shard> shards = new HashSet<Shard>();
	private Map<String, DataSource> dataSources = new HashMap<String, DataSource>();
	private boolean lazyConnectionDataSourceProxy = true;

	public Map<String, DataSource> getDataSources() {
		return dataSources;
	}

	public void afterPropertiesSet() throws Exception {
		if (shards == null || shards.isEmpty()) {
			return;
		}
		for (Shard descriptor : getShards()) {
			Validate.notEmpty(descriptor.getId(), "Shard's id is required.");
			Validate.isTrue(!descriptor.getId().contains(","), "Shard's id can't contains char ','");
			Validate.isTrue(!descriptor.getId().contains(" "), "Shard's id can't contains char ',' ' '");
			Validate.notNull(descriptor.getDataSource());

			DataSource dataSourceToUse = descriptor.getDataSource();
			/**
			 * in multiple data source transaction environment,each data source transaction manager 
			 * begin a transaction will invoke setAutoCommit,setTransactionIsolation on connection, 
			 * these operation will get a connection for pool. To save connection in connection pool,
			 * we warped data source with LazyConnectionDataSourceProxy. this proxy will guarantee that 
			 * only when at least one data access operation occur, the physical connection should be fetched
			 */
			if (isLazyConnectionDataSourceProxy()) {
				dataSourceToUse = new LazyConnectionDataSourceProxy(dataSourceToUse);
				descriptor.setDataSource(dataSourceToUse);
			}
			dataSources.put(descriptor.getId(), dataSourceToUse);
		}
	}

	public Set<Shard> getShards() {
		return shards;
	}

	public void setShards(Set<Shard> shards) {
		this.shards = shards;
	}

	public void setDataSourceDescriptors(Set<Shard> dataSourceDescriptors) {
		this.shards = dataSourceDescriptors;
	}

	@Override
	public DataSource getDataSource(String identity) {
		return dataSources.get(identity);
	}

	public boolean isLazyConnectionDataSourceProxy() {
		return lazyConnectionDataSourceProxy;
	}

	public void setLazyConnectionDataSourceProxy(boolean lazyConnectionDataSourceProxy) {
		this.lazyConnectionDataSourceProxy = lazyConnectionDataSourceProxy;
	}
}

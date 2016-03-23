package com.adanac.framework.dac.client.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adanac.framework.dac.client.support.executor.MappedSqlExecutor;

/**
 * shard隔离类
 * @author adanac
 * @version 1.0
 */
public class ShardingBatchIsolator {
	private Map<MappedSqlExecutor, List<Map<String, Object>>> resources = new HashMap<MappedSqlExecutor, List<Map<String, Object>>>();
	private Map<MappedSqlExecutor, Lock> locks = new HashMap<MappedSqlExecutor, Lock>();
	private Set<MappedSqlExecutor> keys = new HashSet<MappedSqlExecutor>();

	public ShardingBatchIsolator(MappedSqlExecutor[] keys) {
		this(Arrays.asList(keys));
	}

	public ShardingBatchIsolator(Collection<MappedSqlExecutor> keys) {
		if (keys == null || keys.isEmpty()) {
			throw new IllegalArgumentException("empty collection is invalid for hive to spawn data holders.");
		}
		this.keys.addAll(keys);
		initResourceHolders();
		initResourceLocks();
	}

	private void initResourceLocks() {
		for (MappedSqlExecutor key : keys) {
			locks.put(key, new ReentrantLock());
		}
	}

	private void initResourceHolders() {
		for (MappedSqlExecutor key : keys) {
			resources.put(key, new ArrayList<Map<String, Object>>());
		}
	}

	public Map<MappedSqlExecutor, List<Map<String, Object>>> getResources() {
		return this.resources;
	}

	public void emit(MappedSqlExecutor key, Map<String, Object> entity) {
		Lock lock = locks.get(key);
		lock.lock();
		try {
			resources.get(key).add(entity);
		} finally {
			lock.unlock();
		}
	}
}

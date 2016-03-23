package com.adanac.framework.dac.client.support;

import java.util.HashMap;
import java.util.Map;

import com.adanac.framework.dac.route.support.ShardRouter;

/**
 * 分库配置处理
 * @author adanac
 * @version 1.0
 */
public class ShardingConfigurationUtil {
	public static final String SHARDROUTER_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME = "_shard_Router_";

	public static final String SQLRMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME = "_sql_Shard_Mapping_";

	public static final String NAMESPACEMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME = "_namespace_Shard_Mapping_";

	/**
	 * 功能描述：将StatementId及其分库配置存入configuration对象
	 * 输入参数：配置对象，sqlId，分库规则id
	 */
	public static void addSqlShardMapping(Configuration configuration, String statementId, String shardRouterId) {
		@SuppressWarnings("unchecked")
		Map<String, String> sqlShardMapping = (Map<String, String>) configuration
				.getAttribute(SQLRMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);
		if (sqlShardMapping == null) {
			sqlShardMapping = new HashMap<String, String>();
			configuration.addAttribute(SQLRMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, sqlShardMapping);
		}
		sqlShardMapping.put(statementId, shardRouterId);

	}

	/**
	 * 功能描述：根据StatementId从configuration对象读取出对应的分库配置
	 * 输入参数：配置对象，sqlId
	 * @param configuration,statementId
	 * 返回值：分库规则id
	 */
	public static String getStatementMappedShardRef(Configuration configuration, String statementId) {
		@SuppressWarnings("unchecked")
		Map<String, String> sqlShardMapping = (Map<String, String>) configuration
				.getAttribute(SQLRMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);
		if (sqlShardMapping == null) {
			sqlShardMapping = new HashMap<String, String>();
			configuration.addAttribute(SQLRMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, sqlShardMapping);
		}
		return sqlShardMapping.get(statementId);

	}

	/**
	 * 功能描述：根据StatementId获取出对应的分库规则
	 * 输入参数：配置对象，sqlId
	 * @param configuration,statementId
	 * 返回值：分库规则对象
	 */
	public static ShardRouter getStatementMappedShardRouter(Configuration configuration, String statementId) {
		String shardRef = getStatementMappedShardRef(configuration, statementId);
		return getShardRouter(configuration, shardRef);
	}

	/**
	 * 功能描述：将namespace及其分库配置存入configuration对象
	 * 输入参数：配置对象，namespace，分库规则id
	 * @param configuration,namespace,shardRouterId
	 */
	public static String addNamespaceShardMapping(Configuration configuration, String namespace, String shardRouterId) {
		@SuppressWarnings("unchecked")
		Map<String, String> sqlShardMapping = (Map<String, String>) configuration
				.getAttribute(NAMESPACEMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);
		if (sqlShardMapping == null) {
			sqlShardMapping = new HashMap<String, String>();
			configuration.addAttribute(NAMESPACEMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, sqlShardMapping);
		}
		return sqlShardMapping.put(namespace, shardRouterId);

	}

	/**
	 * 功能描述：根据namespace从configuration对象读取出对应的分库配置
	 * 输入参数：配置对象，namespace
	 * @param configuration,namespace
	 * 返回值：分库规则id
	 */
	public static String getNamespaceShardRef(Configuration configuration, String namespace) {
		@SuppressWarnings("unchecked")
		Map<String, String> sqlShardMapping = (Map<String, String>) configuration
				.getAttribute(NAMESPACEMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);
		if (sqlShardMapping == null) {
			sqlShardMapping = new HashMap<String, String>();
			configuration.addAttribute(NAMESPACEMAPPING_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, sqlShardMapping);
		}
		return sqlShardMapping.get(namespace);
	}

	/**
	 * 功能描述：根据 namespace获取出对应的分库规则
	 * 输入参数：配置对象，namespace
	 */
	public static ShardRouter getNamespaceMappedShardRouter(Configuration configuration, String namespace) {
		String shardRef = getNamespaceShardRef(configuration, namespace);
		return getShardRouter(configuration, shardRef);
	}

	/**
	 * 功能描述：添加shard组到configuration对象
	 * 输入参数：配置对象，分库路由规则对象
	 */
	public static void addShardRouter(Configuration configuration, ShardRouter shardRouter) {

		@SuppressWarnings("unchecked")
		Map<String, ShardRouter> shardRouters = (Map<String, ShardRouter>) configuration
				.getAttribute(SHARDROUTER_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);

		if (shardRouters == null) {
			shardRouters = new HashMap<String, ShardRouter>();
			configuration.addAttribute(SHARDROUTER_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, shardRouters);
		}
		shardRouters.put(shardRouter.getId(), shardRouter);
	}

	/**
	 * 功能描述：根据id从configuration对象中获取对应的shard组
	 * 输入参数：配置对象，分库路由规则id
	 */
	public static ShardRouter getShardRouter(Configuration configuration, String id) {
		@SuppressWarnings("unchecked")
		Map<String, ShardRouter> shardRouters = (Map<String, ShardRouter>) configuration
				.getAttribute(SHARDROUTER_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME);

		if (shardRouters == null) {
			shardRouters = new HashMap<String, ShardRouter>();
			configuration.addAttribute(SHARDROUTER_HOLD_ON_CONFIGURATION_ATTRIBUTE_NAME, shardRouters);
		}
		return shardRouters.get(id);
	}
}

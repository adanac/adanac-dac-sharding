package com.adanac.framework.dac.parsing.annotation;

import com.adanac.framework.dac.client.support.Configuration;
import com.adanac.framework.dac.client.support.ShardingConfigurationUtil;
import com.adanac.framework.dac.parsing.builder.BaseBuilder;
import com.adanac.framework.dac.parsing.exception.ParsingException;
import com.adanac.framework.dac.route.annotation.ShardMapping;

/**
 * 解析ShardMapping注解
 * @author adanac
 * @version 1.0
 */
public class AnnotationShardRefBuilder extends BaseBuilder {
	private Class<?> entityClass;
	private String currentNamespace;

	public AnnotationShardRefBuilder(Configuration configuration, Class<?> entityClass) {
		super(configuration);
		this.entityClass = entityClass;
		if (entityClass == null) {
			throw new ParsingException("entityClass can't null.");
		}
		this.currentNamespace = entityClass.getName();
	}

	/**
	 * 功能描述：解析ShardMapping注解
	 */
	public void parse() {

		if (!entityClass.isAnnotationPresent(ShardMapping.class)) {
			return;
		}
		ShardMapping shardMapping = entityClass.getAnnotation(ShardMapping.class);
		String shardRef = shardMapping.shardRef();
		if ("".equals(shardRef)) {
			throw new ParsingException(
					"Annotation ShardMapping at " + entityClass.getName() + " must  specify a ShardRouter");
		} else if (ShardingConfigurationUtil.getShardRouter(configuration, shardRef) == null) {
			throw new ParsingException(
					"Annotation ShardMapping at " + entityClass.getName() + " must  specify a ShardRouter");
		}
		String[] methods = shardMapping.methods();

		for (String method : methods) {
			if ("persist".equals(method)) {
				ShardingConfigurationUtil.addSqlShardMapping(configuration, applyCurrentNamespace("insert"), shardRef);
			} else if ("merge".equals(method)) {
				ShardingConfigurationUtil.addSqlShardMapping(configuration, applyCurrentNamespace("update"), shardRef);
			} else if ("dynamicMerge".equals(method)) {
				ShardingConfigurationUtil.addSqlShardMapping(configuration, applyCurrentNamespace("updateDynamic"),
						shardRef);
			} else if ("remove".equals(method)) {
				ShardingConfigurationUtil.addSqlShardMapping(configuration, applyCurrentNamespace("delete"), shardRef);
			} else if ("find".equals(method)) {
				ShardingConfigurationUtil.addSqlShardMapping(configuration, applyCurrentNamespace("select"), shardRef);
			} else {
				throw new ParsingException("Annotation ShardMapping at " + entityClass.getName()
						+ " can't support method: " + method + " reference a ShardRouter.");
			}
		}

	}

	/**
	 * 功能描述：返回带命名空间的sqlId<br>
	 * 输入参数：sqlId
	 * @param sqlId
	 * 返回值:  sqlId <说明> 
	 * @return String
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private String applyCurrentNamespace(String id) {
		if (id.startsWith(currentNamespace + ".")) {
			return id;
		} else {
			return currentNamespace + "." + id;
		}
	}
}

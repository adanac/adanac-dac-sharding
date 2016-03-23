package com.adanac.framework.dac.parsing.xml;

import java.io.InputStream;
import java.util.List;

import com.adanac.framework.dac.client.support.Configuration;
import com.adanac.framework.dac.client.support.ShardingConfigurationUtil;
import com.adanac.framework.dac.parsing.builder.BaseBuilder;
import com.adanac.framework.dac.parsing.exception.ParsingException;
import com.adanac.framework.dac.route.support.DefaultShardRouter;
import com.adanac.framework.dac.route.support.ShardRouter;
import com.adanac.framework.dac.route.xmltags.DynamicTagSource;
import com.adanac.framework.dac.route.xmltags.XMLTagBuilder;

/**
 * shard配置文件解析类
 * @author adanac
 * @version 1.0
 */
public class XmlShardBuilder extends BaseBuilder {
	private XPathParser parser;
	private String resource;

	public XmlShardBuilder(InputStream inputStream, Configuration configuration, String resource) {
		this(new XPathParser(inputStream, false, configuration.getVariables(), new XmlShardEntityResolver()),
				configuration, resource);
	}

	private XmlShardBuilder(XPathParser parser, Configuration configuration, String resource) {
		super(configuration);
		this.parser = parser;
		this.resource = resource;
	}

	public void parse() {
		if (!configuration.isResourceLoaded(resource)) {
			configurationElement(parser.evalNode("/sharding"));
			configuration.addLoadedResource(resource);
		}
	}

	/**
	 * 功能描述：解析shard配置文件下的子标签<br>
	 * 输入参数：xml节点<按照参数定义顺序> 
	 * @param context
	 * 返回值:  <说明> 
	 * @return 返回值
	 * @throw Exception
	 * @see 需要参见的其它内容
	 */
	private void configurationElement(XNode context) {
		try {
			shardRouterElement(context.evalNodes("/sharding/shardRouter"));
			// shardRouterElement(context.evalNodes("/sharding/tableRouter"));
			shardMappingElement(context.evalNodes("/sharding/shardMapping"));
		} catch (Exception e) {
			throw new ParsingException("Error parsing sharding XML " + resource + " Cause: " + e, e);
		}
	}

	/**
	 * 功能描述：详细解析子标签shardRouter<br>
	 * 输入参数：list集合，元素是xml节点<按照参数定义顺序> 
	 * @param list
	 * 返回值:  <说明> 
	 * @return 返回值
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private void shardRouterElement(List<XNode> list) throws Exception {
		DefaultShardRouter shardRouter;
		for (XNode shardRouterNode : list) {
			String id = shardRouterNode.getStringAttribute("id");
			if (id == null || "".equals(id)) {
				throw new ParsingException("Error parsing sharding XML. " + resource
						+ " Cause: the 'shardRouter' element id is required.");
			}
			XMLTagBuilder builder = new XMLTagBuilder(configuration, shardRouterNode);

			DynamicTagSource tagSource = builder.parseScriptNode();

			shardRouter = new DefaultShardRouter();
			shardRouter.setId(id);
			shardRouter.setTagSource(tagSource);
			addShardRouter(shardRouter);
		}

	}

	/**
	 * 功能描述：添加shardRouter对象到configuration对象<br>
	 * 输入参数：分库规则对象<按照参数定义顺序> 
	 * @param shardRouter
	 * 返回值:  <说明> 
	 * @return 返回值
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private void addShardRouter(ShardRouter shardRouter) {
		if (ShardingConfigurationUtil.getShardRouter(configuration, shardRouter.getId()) != null) {
			throw new ParsingException(
					"Parsing " + resource + " find duplicate ShardRouter id '" + shardRouter.getId() + "'");
		}
		ShardingConfigurationUtil.addShardRouter(configuration, shardRouter);
	}

	/**
	 * 功能描述：详细解析子标签shardMapping，并添加到configuration对象中<br>
	 * 输入参数：list集合，元素是xml节点<按照参数定义顺序> 
	 * @param list
	 * 返回值:  <说明> 
	 * @return 返回值
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private void shardMappingElement(List<XNode> list) throws Exception {
		for (XNode shardMappingNode : list) {
			List<XNode> children = shardMappingNode.getChildren();

			String statement = null;
			String namespace = null;
			String shardref = null;
			for (XNode chid : children) {
				if ("shard-ref".equals(chid.getName())) {
					shardref = chid.getStringBody();
				} else if ("statement".equals(chid.getName())) {
					statement = chid.getStringBody();
				} else if ("namespace".equals(chid.getName())) {
					namespace = chid.getStringBody();
				} else if ("shard-ref".equals(chid.getName())) {
					shardref = chid.getStringBody();
				}
			}
			if (shardref == null || "".equals(shardref)) {
				throw new ParsingException("Error parsing sharding XML. " + resource
						+ " Cause: the 'shardMapping' element must ref a correct ShardRouter.");
			}
			if (statement != null && !"".equals(statement)) {
				if (statement.contains(",")) {
					String[] statements = statement.split(",");
					for (String aStatement : statements) {
						ShardingConfigurationUtil.addSqlShardMapping(configuration, aStatement.trim(), shardref);
					}
				} else {
					ShardingConfigurationUtil.addSqlShardMapping(configuration, statement.trim(), shardref);
				}
			} else if (namespace != null && !"".equals(namespace)) {
				if (namespace.contains(",")) {
					String[] namespaces = namespace.split(",");
					for (String aNamespace : namespaces) {
						ShardingConfigurationUtil.addNamespaceShardMapping(configuration, aNamespace.trim(), shardref);
					}
				} else {
					ShardingConfigurationUtil.addNamespaceShardMapping(configuration, namespace.trim(), shardref);
				}
			} else {
				throw new ParsingException("Error parsing sharding XML. " + resource
						+ " Cause: the 'shardMapping' element must ref a correct ShardRouter.");
			}
		}
	}
}

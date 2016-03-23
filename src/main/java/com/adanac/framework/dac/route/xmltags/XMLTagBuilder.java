package com.adanac.framework.dac.route.xmltags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.adanac.framework.dac.client.support.Configuration;
import com.adanac.framework.dac.parsing.builder.BaseBuilder;
import com.adanac.framework.dac.parsing.exception.ParsingException;
import com.adanac.framework.dac.parsing.xml.XNode;
import com.adanac.framework.dac.parsing.xml.XPathParser;
import com.adanac.framework.dac.parsing.xml.XmlShardEntityResolver;

/**
 * xml标签处理类
 * @author adanac
 * @version 1.0
 */
public class XMLTagBuilder extends BaseBuilder {
	private XNode context;

	public XMLTagBuilder(Configuration configuration, XNode context) {
		super(configuration);
		this.context = context;
	}

	public XMLTagBuilder(Configuration configuration, String context) {
		super(configuration);
		XPathParser parser = new XPathParser(context, false, configuration.getVariables(),
				new XmlShardEntityResolver());
		this.context = parser.evalNode("/script");
	}

	/**
	 * 功能描述：处理脚本标签<br>
	 */
	public DynamicTagSource parseScriptNode() {
		List<TagNode> contents = parseDynamicTags(context);
		MixedTagNode rootSqlNode = new MixedTagNode(contents);
		return new DynamicTagSource(configuration, rootSqlNode);
	}

	/**
	 * 功能描述：处理动态标签<br>
	 * 输入参数：xml节点<按照参数定义顺序> 
	 * @param node
	 * 返回值:  list集合，元素是标签节点 <说明> 
	 * @return List<TagNode>
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private List<TagNode> parseDynamicTags(XNode node) {
		List<TagNode> contents = new ArrayList<TagNode>();
		NodeList children = node.getNode().getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			XNode child = node.newXNode(children.item(i));
			String nodeName = child.getNode().getNodeName();
			if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE
					|| child.getNode().getNodeType() == Node.TEXT_NODE) {
				String data = child.getStringBody("");
				contents.add(new StaticTextTagNode(data));
			} else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) {
				NodeHandler handler = nodeHandlers.get(nodeName);
				if (handler == null) {
					throw new ParsingException("Unknown element <" + nodeName + "> in shard statement.");
				}
				handler.handleNode(child, contents);
			}
		}
		return contents;
	}

	private Map<String, NodeHandler> nodeHandlers = new HashMap<String, NodeHandler>() {
		private static final long serialVersionUID = 7123056019193266281L;

		{
			put("if", new IfHandler());
			put("choose", new ChooseHandler());
			put("when", new IfHandler());
			put("otherwise", new OtherwiseHandler());
			put("interval", new IntervalHandler());
			put("shard", new IntervalShardHandler());
		}
	};

	/**
	 * 标签处理接口<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private interface NodeHandler {
		void handleNode(XNode nodeToHandle, List<TagNode> targetContents);
	}

	/**
	 * if标签处理类<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private class IfHandler implements NodeHandler {
		public void handleNode(XNode nodeToHandle, List<TagNode> targetContents) {
			List<TagNode> contents = parseDynamicTags(nodeToHandle);
			MixedTagNode mixedSqlNode = new MixedTagNode(contents);
			String test = nodeToHandle.getStringAttribute("test");
			IfTagNode ifSqlNode = new IfTagNode(mixedSqlNode, test);
			targetContents.add(ifSqlNode);
		}
	}

	/**
	 * otherwise标签处理类<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private class OtherwiseHandler implements NodeHandler {
		public void handleNode(XNode nodeToHandle, List<TagNode> targetContents) {
			List<TagNode> contents = parseDynamicTags(nodeToHandle);
			MixedTagNode mixedSqlNode = new MixedTagNode(contents);
			targetContents.add(mixedSqlNode);
		}
	}

	/**
	 * choose标签处理类<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private class ChooseHandler implements NodeHandler {
		public void handleNode(XNode nodeToHandle, List<TagNode> targetContents) {
			List<TagNode> whenTagNodes = new ArrayList<TagNode>();
			List<TagNode> otherwiseTagNodes = new ArrayList<TagNode>();
			handleWhenOtherwiseNodes(nodeToHandle, whenTagNodes, otherwiseTagNodes);
			TagNode defaultTagNode = getDefaultTagNode(otherwiseTagNodes);
			ChooseTagNode chooseSqlNode = new ChooseTagNode(whenTagNodes, defaultTagNode);
			targetContents.add(chooseSqlNode);
		}

		private void handleWhenOtherwiseNodes(XNode chooseTagNode, List<TagNode> ifTagNodes,
				List<TagNode> defaultTagNodes) {
			List<XNode> children = chooseTagNode.getChildren();
			for (XNode child : children) {
				String nodeName = child.getNode().getNodeName();
				NodeHandler handler = nodeHandlers.get(nodeName);
				if (handler instanceof IfHandler) {
					handler.handleNode(child, ifTagNodes);
				} else if (handler instanceof OtherwiseHandler) {
					handler.handleNode(child, defaultTagNodes);
				}
			}
		}

		private TagNode getDefaultTagNode(List<TagNode> defaultSqlNodes) {
			TagNode defaultSqlNode = null;
			if (defaultSqlNodes.size() == 1) {
				defaultSqlNode = defaultSqlNodes.get(0);
			} else if (defaultSqlNodes.size() > 1) {
				throw new ParsingException("Too many default (otherwise) elements in choose statement.");
			}
			return defaultSqlNode;
		}
	}

	/**
	 * interval标签处理类<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private class IntervalHandler implements NodeHandler {
		public void handleNode(XNode nodeToHandle, List<TagNode> targetContents) {
			Integer start = nodeToHandle.getIntAttribute("start");
			Integer end = nodeToHandle.getIntAttribute("end");
			Integer mod = nodeToHandle.getIntAttribute("mod");
			String param = nodeToHandle.getStringAttribute("param");

			List<TagNode> intervalShardNodes = new ArrayList<TagNode>();
			handleIntervalShardNodes(nodeToHandle, intervalShardNodes);
			setIntervalShardNodes(intervalShardNodes, param, mod);
			IntervalTagNode intervalTagNode = new IntervalTagNode(intervalShardNodes, start, end, param);
			targetContents.add(intervalTagNode);
		}

		private void handleIntervalShardNodes(XNode intervalTagNode, List<TagNode> intervalShardNodes) {
			List<XNode> children = intervalTagNode.getChildren();
			for (XNode child : children) {
				String nodeName = child.getNode().getNodeName();
				NodeHandler handler = nodeHandlers.get(nodeName);
				if (handler instanceof IntervalShardHandler) {
					handler.handleNode(child, intervalShardNodes);
				}
			}
		}

		private void setIntervalShardNodes(List<TagNode> defaultSqlNodes, String param, Integer mod) {
			for (TagNode tagNode : defaultSqlNodes) {
				if (tagNode instanceof IntervalShardTagNode) {
					IntervalShardTagNode intervalShardTagNode = (IntervalShardTagNode) tagNode;
					intervalShardTagNode.setParam(param);
					intervalShardTagNode.setMod(mod);
				} else {
					throw new ParsingException("Can't support tag node type in <interval>");
				}
			}
		}
	}

	/**
	 * interval中shard标签处理类<br> 
	 * @author 13071496
	 * @see [相关类/方法]（可选）
	 * @since [产品/模块版本] （可选）
	 */
	private class IntervalShardHandler implements NodeHandler {
		public void handleNode(XNode nodeToHandle, List<TagNode> targetContents) {
			List<TagNode> contents = parseDynamicTags(nodeToHandle);
			MixedTagNode mixedSqlNode = new MixedTagNode(contents);
			String resultString = nodeToHandle.getStringAttribute("result");
			Set<Integer> result = getResult(resultString);
			IntervalShardTagNode intervalTagNode = new IntervalShardTagNode(mixedSqlNode, result);
			targetContents.add(intervalTagNode);
		}

		private Set<Integer> getResult(String resultString) {
			Set<Integer> result = new HashSet<Integer>();
			if (resultString == null || "".equals(resultString)) {
				return null;
			}
			String[] results = resultString.split(",");
			for (String number : results) {
				try {
					result.add(Integer.valueOf(number));
				} catch (NumberFormatException e) {
					throw new ParsingException("The hash attribute of element <shard> must a set of int.");
				}
			}
			return result;
		}
	}
}

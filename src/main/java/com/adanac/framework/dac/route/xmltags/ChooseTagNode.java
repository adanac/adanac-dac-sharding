package com.adanac.framework.dac.route.xmltags;

import java.util.List;

/**
 * choose标签节点
 * @author adanac
 * @version 1.0
 */
public class ChooseTagNode implements TagNode {
	private TagNode defaultSqlNode;
	private List<TagNode> ifSqlNodes;

	public ChooseTagNode(List<TagNode> ifSqlNodes, TagNode defaultSqlNode) {
		this.ifSqlNodes = ifSqlNodes;
		this.defaultSqlNode = defaultSqlNode;
	}

	public boolean apply(EvaluationContext context) {
		for (TagNode sqlNode : ifSqlNodes) {
			if (sqlNode.apply(context)) {
				return true;
			}
		}
		if (defaultSqlNode != null) {
			defaultSqlNode.apply(context);
			return true;
		}
		return false;
	}
}

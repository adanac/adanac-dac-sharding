package com.adanac.framework.dac.route.xmltags;

import java.util.List;

/**
 * 混合标签节点
 * @author adanac
 * @version 1.0
 */
public class MixedTagNode implements TagNode {
	private List<TagNode> contents;

	public MixedTagNode(List<TagNode> contents) {
		this.contents = contents;
	}

	public boolean apply(EvaluationContext context) {
		for (TagNode sqlNode : contents) {
			sqlNode.apply(context);
		}
		return true;
	}
}

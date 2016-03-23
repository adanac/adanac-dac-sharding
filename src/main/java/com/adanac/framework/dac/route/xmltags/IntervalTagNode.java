package com.adanac.framework.dac.route.xmltags;

import java.util.List;

/**
 * interval标签节点
 * @author adanac
 * @version 1.0
 */
public class IntervalTagNode implements TagNode {
	private Integer start;

	private Integer end;

	private String param;

	private List<TagNode> intervalShardTagNodes;

	private ExpressionEvaluator evaluator;

	public IntervalTagNode(List<TagNode> intervalShardTagNodes, Integer start, Integer end, String param) {
		this.intervalShardTagNodes = intervalShardTagNodes;
		this.start = start;
		this.end = end;
		this.param = param;
		this.evaluator = new ExpressionEvaluator();
	}

	public boolean apply(EvaluationContext context) {
		String expression = param + " >= " + start + " && " + param + " <= " + end;
		if (evaluator.evaluateBoolean(expression, context.getParameterObject())) {
			for (TagNode sqlNode : intervalShardTagNodes) {
				if (sqlNode.apply(context)) {
					return true;
				}
			}
		}
		return false;
	}
}

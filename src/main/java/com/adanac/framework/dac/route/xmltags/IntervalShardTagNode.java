package com.adanac.framework.dac.route.xmltags;

import java.util.Set;

/**
 * shard标签节点
 * @author adanac
 * @version 1.0
 */
public class IntervalShardTagNode implements TagNode {
	private TagNode contents;

	private Set<Integer> result;

	private String param;

	private Integer mod;

	private ExpressionEvaluator evaluator;

	public IntervalShardTagNode(TagNode contents, Set<Integer> result) {
		super();
		this.contents = contents;
		this.result = result;
		this.evaluator = new ExpressionEvaluator();
	}

	public boolean apply(EvaluationContext context) {
		String expression = param + " % " + mod;// 取模
		Object value = evaluator.evaluateObject(expression, context.getParameterObject());
		if (value != null && value instanceof Number) {
			if (result.contains(Integer.valueOf(value.toString()))) {
				contents.apply(context);
				return true;
			}
		}
		return false;
	}

	public void setParam(String param) {
		this.param = param;
	}

	public void setMod(Integer mod) {
		this.mod = mod;
	}
}

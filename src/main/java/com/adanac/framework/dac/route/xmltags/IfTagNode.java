package com.adanac.framework.dac.route.xmltags;

/**
 * if标签节点
 * @author adanac
 * @version 1.0
 */
public class IfTagNode implements TagNode {
	private ExpressionEvaluator evaluator;
	private String test;
	private TagNode contents;

	public IfTagNode(TagNode contents, String test) {
		this.test = test;
		this.contents = contents;
		this.evaluator = new ExpressionEvaluator();
	}

	public boolean apply(EvaluationContext context) {
		if (evaluator.evaluateBoolean(test, context.getParameterObject())) {
			contents.apply(context);
			return true;
		}
		return false;
	}
}

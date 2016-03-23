package com.adanac.framework.dac.route.xmltags;

/**
 * 静态文本标签节点
 * @author adanac
 * @version 1.0
 */
public class StaticTextTagNode implements TagNode {
	private String text;

	public StaticTextTagNode(String text) {
		this.text = text;
	}

	public boolean apply(EvaluationContext context) {
		context.appendText(text);
		return true;
	}
}

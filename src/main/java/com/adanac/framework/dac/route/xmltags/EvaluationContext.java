package com.adanac.framework.dac.route.xmltags;

import com.adanac.framework.dac.client.support.Configuration;

/**
 * 评估环境
 * @author adanac
 * @version 1.0
 */
public class EvaluationContext {
	private final Object parameterObject;
	private final StringBuilder textBuilder = new StringBuilder();
	private int uniqueNumber = 0;

	public EvaluationContext(Configuration configuration, Object parameterObject) {
		this.parameterObject = parameterObject;
	}

	public Object getParameterObject() {
		return parameterObject;
	}

	public void appendText(String text) {
		textBuilder.append(text);
		textBuilder.append(" ");
	}

	public String getText() {
		return textBuilder.toString().trim();
	}

	public int getUniqueNumber() {
		return uniqueNumber++;
	}
}

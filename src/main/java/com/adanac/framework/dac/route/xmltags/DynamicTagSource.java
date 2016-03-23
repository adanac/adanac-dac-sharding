package com.adanac.framework.dac.route.xmltags;

import com.adanac.framework.dac.client.support.Configuration;

/**
 * 动态标签源
 * @author adanac
 * @version 1.0
 */
public class DynamicTagSource {
	private Configuration configuration;
	private TagNode rootSqlNode;

	public DynamicTagSource(Configuration configuration, TagNode rootSqlNode) {
		this.configuration = configuration;
		this.rootSqlNode = rootSqlNode;
	}

	public String evaluate(Object parameterObject) {
		EvaluationContext context = new EvaluationContext(configuration, parameterObject);
		rootSqlNode.apply(context);
		return context.getText();
	}
}

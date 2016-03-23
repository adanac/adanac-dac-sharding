package com.adanac.framework.dac.route.xmltags;

import java.math.BigDecimal;

/**
 * 表达式求值类
 * @author adanac
 * @version 1.0
 */
public class ExpressionEvaluator {
	public Object evaluateObject(String expression, Object parameterObject) {
		Object value = OgnlCache.getValue(expression, parameterObject);
		return value;
	}

	public boolean evaluateBoolean(String expression, Object parameterObject) {
		Object value = OgnlCache.getValue(expression, parameterObject);
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		if (value instanceof Number) {
			return !new BigDecimal(String.valueOf(value)).equals(BigDecimal.ZERO);
		}
		return value != null;
	}
}

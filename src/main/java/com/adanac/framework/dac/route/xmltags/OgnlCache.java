package com.adanac.framework.dac.route.xmltags;

import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.adanac.framework.dac.parsing.exception.ParsingException;

import ognl.ExpressionSyntaxException;
import ognl.Node;
import ognl.Ognl;
import ognl.OgnlException;
import ognl.OgnlParser;
import ognl.ParseException;
import ognl.TokenMgrError;

/**
 * ognl表达式处理
 * @author adanac
 * @version 1.0
 */
public class OgnlCache {
	private static final Map<String, ognl.Node> expressionCache = new ConcurrentHashMap<String, ognl.Node>();

	public static Object getValue(String expression, Object root) {
		try {
			return Ognl.getValue(parseExpression(expression), root);
		} catch (OgnlException e) {
			throw new ParsingException("Error evaluating expression '" + expression + "'. Cause: " + e, e);
		}
	}

	private static Object parseExpression(String expression) throws OgnlException {
		try {
			Node node = expressionCache.get(expression);
			if (node == null) {
				node = new OgnlParser(new StringReader(expression)).topLevelExpression();
				expressionCache.put(expression, node);
			}
			return node;
		} catch (ParseException e) {
			throw new ExpressionSyntaxException(expression, e);
		} catch (TokenMgrError e) {
			throw new ExpressionSyntaxException(expression, e);
		}
	}
}

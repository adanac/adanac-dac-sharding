package com.adanac.framework.dac.route.support;

import java.util.HashSet;
import java.util.Set;

import com.adanac.framework.dac.route.xmltags.DynamicTagSource;

/**
 * 默认shard路由
 * @author adanac
 * @version 1.0
 */
public class DefaultShardRouter implements ShardRouter {
	private String id;

	private DynamicTagSource tagSource;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public DynamicTagSource getTagSource() {
		return tagSource;
	}

	public void setTagSource(DynamicTagSource tagSource) {
		this.tagSource = tagSource;
	}

	public String[] processRoute(Object param) {
		Set<String> result = new HashSet<String>();
		String evaluated = tagSource.evaluate(param);
		if (evaluated == null || "".equals(evaluated)) {
			return new String[] {};
		}
		evaluated = evaluated.replace(',', ' ');
		String[] texts = evaluated.split(" ");
		for (String text : texts) {
			if (text == null || "".equals(text.trim())) {
				continue;
			}
			result.add(text.trim());
		}
		return result.toArray(new String[result.size()]);
	}
}

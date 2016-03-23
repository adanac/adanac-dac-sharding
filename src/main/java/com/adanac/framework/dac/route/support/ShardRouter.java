package com.adanac.framework.dac.route.support;

/**
 * shard路由接口
 * @author adanac
 * @version 1.0
 */
public interface ShardRouter {

	String[] processRoute(Object param);

	String getId();
}

package com.adanac.framework.dac.route.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * ShardMapping注解<br>
 * DAL-API操作需要分库功能时，需要在实体bean上添加此注解
 * @author adanac
 * @version 1.0
 */
@Target({ TYPE })
@Retention(RUNTIME)
public @interface ShardMapping {
	/**
	 * shard路由规则的名称id
	 * @return
	 */
	String shardRef() default "";

	/**
	 * 方法名称
	 * @return
	 */
	String[]methods() default { "persist", "merge", "dynamicMerge", "remove", "find" };
}

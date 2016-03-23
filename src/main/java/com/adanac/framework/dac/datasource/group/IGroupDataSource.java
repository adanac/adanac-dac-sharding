package com.adanac.framework.dac.datasource.group;

import javax.sql.DataSource;

/**
 * 分组数据源
 * @author adanac
 * @version 1.0
 */
public interface IGroupDataSource extends DataSource {
	/**
	 * 功能描述: 根据SQL特性获取原生类型数据源
	 * @param sqlBean SQL模板映射对象
	 */
	DataSource getDataSource(String sqlBean);
}

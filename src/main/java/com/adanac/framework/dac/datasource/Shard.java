package com.adanac.framework.dac.datasource;

import java.io.Serializable;

import javax.sql.DataSource;

/**
 * 功能描述：节点类<br>
 * 一个节点对应一个数据源
 * @author adanac
 * @version 1.0
 */
public class Shard implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4514971552821936513L;

	private String id;

	private DataSource dataSource;

	private String description;

	public String getId() {
		return this.id;
	}

	public void setId(String identity) {
		this.id = identity;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Shard other = (Shard) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "DataSourceDescriptor [identity=" + id + ", " + "dataSource=" + dataSource + "]";
	}
}

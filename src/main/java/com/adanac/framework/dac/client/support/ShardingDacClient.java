package com.adanac.framework.dac.client.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.persistence.Entity;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.core.io.Resource;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.util.Assert;

import com.adanac.framework.dac.cache.annotation.CacheController;
import com.adanac.framework.dac.client.support.executor.MappedSqlExecutor;
import com.adanac.framework.dac.datasource.Shard;
import com.adanac.framework.dac.datasource.ShardRegister;
import com.adanac.framework.dac.exception.DalException;
import com.adanac.framework.dac.parsing.annotation.AnnotationCacheBuilder;
import com.adanac.framework.dac.parsing.annotation.AnnotationShardRefBuilder;
import com.adanac.framework.dac.parsing.annotation.AnnotationSqlMapBuilder;
import com.adanac.framework.dac.parsing.exception.ParsingException;
import com.adanac.framework.dac.parsing.io.ResolverUtil;
import com.adanac.framework.dac.parsing.support.annotation.TableRoute;
import com.adanac.framework.dac.parsing.xml.XmlShardBuilder;
import com.adanac.framework.dac.parsing.xml.XmlSqlMapBuilder;
import com.adanac.framework.dac.route.annotation.ShardMapping;
import com.adanac.framework.dac.route.exception.RouteException;
import com.adanac.framework.dac.route.support.ShardRouter;
import com.adanac.framework.dac.util.DacUtils;

/**
 * 分库客户端
 * @author adanac
 * @version 1.0
 */
public class ShardingDacClient extends DefaultDacClient {
	protected Resource[] shardingConfigLocation;

	protected ShardRegister shardRegister;

	protected Map<String, MappedSqlExecutor> shardMappedSqlExectorMapping = new HashMap<String, MappedSqlExecutor>();

	protected String defualtShardName;

	public ShardRegister getShardRegister() {
		return shardRegister;
	}

	public void setShardRegister(ShardRegister shardRegister) {
		this.shardRegister = shardRegister;
	}

	public Resource[] getShardingConfigLocation() {
		return shardingConfigLocation;
	}

	public void setShardingConfigLocation(Resource[] shardingConfigLocation) {
		this.shardingConfigLocation = shardingConfigLocation;
	}

	public String getDefualtShardName() {
		return defualtShardName;
	}

	public void setDefualtShardName(String defualtShardName) {
		this.defualtShardName = defualtShardName;
	}

	/*----------------------------DalClient method area------------------------------*/
	@Override
	public Number persist(Object entity) {
		return persist(entity, Number.class);
	}

	@Override
	public <T> T persist(Object entity, Class<T> requiredType) {
		assertMapped(entity);
		Class<? extends Object> entityClass = entity.getClass();
		String sqlId = entityClass.getName() + ".insert";

		MappedStatement mappedStatement = configuration.getMappedStatement(sqlId, true);
		// 查询主键操作在执行分库之前
		if (mappedStatement.getKeyGenerator() != null) {
			MappedSqlExecutor defualtShard = shardMappedSqlExectorMapping.get(defualtShardName);
			Object seq = defualtShard.queryBySequence(mappedStatement.getKeyGenerator(), false);
			DacUtils.setProperty(entity, mappedStatement.getIdProperty(), seq);
		}

		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, entity);
		// 如果路由出不止一个shard，则报错
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).persist(entity, requiredType);
	}

	@Override
	public int merge(Object entity) {
		assertMapped(entity);
		Class<? extends Object> entityClass = entity.getClass();
		String sqlId = entityClass.getName() + ".update";
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, entity);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).merge(entity);
	}

	@Override
	public int dynamicMerge(Object entity) {
		assertMapped(entity);
		Class<? extends Object> entityClass = entity.getClass();
		String sqlId = entityClass.getName() + ".updateDynamic";
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, entity);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).dynamicMerge(entity);
	}

	@Override
	public int remove(Object entity) {
		assertMapped(entity);
		Class<? extends Object> entityClass = entity.getClass();
		String sqlId = entityClass.getName() + ".delete";
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, entity);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).remove(entity);
	}

	@Override
	public <T> T find(Class<T> entityClass, Object entity) {
		assertMapped(entity);
		String sqlId = entityClass.getName() + ".select";
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, entity);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).find(entityClass, entity);
	}

	@Override
	public <T> T queryForObject(String sqlId, Map<String, Object> paramMap, Class<T> requiredType) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForObject(sqlId, paramMap, requiredType);
	}

	@Override
	public <T> T queryForObject(String sqlId, Object param, Class<T> requiredType) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForObject(sqlId, param, requiredType);
	}

	@Override
	public <T> T queryForObject(String sqlId, Map<String, Object> paramMap, RowMapper<T> rowMapper) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForObject(sqlId, paramMap, rowMapper);
	}

	@Override
	public <T> T queryForObject(String sqlId, Object param, RowMapper<T> rowMapper) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForObject(sqlId, param, rowMapper);
	}

	@Override
	public Map<String, Object> queryForMap(String sqlId, Map<String, Object> paramMap) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForMap(sqlId, paramMap);
	}

	@Override
	public Map<String, Object> queryForMap(String sqlId, Object param) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForMap(sqlId, param);
	}

	@Override
	public <T> List<T> queryForList(String sqlId, Map<String, Object> paramMap, Class<T> requiredType) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, paramMap, requiredType);
	}

	@Override
	public <T> List<T> queryForList(String sqlId, Object param, Class<T> requiredType) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, param, requiredType);
	}

	@Override
	public List<Map<String, Object>> queryForList(String sqlId, Map<String, Object> paramMap) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, paramMap);
	}

	@Override
	public List<Map<String, Object>> queryForList(String sqlId, Object param) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, param);
	}

	@Override
	public <T> List<T> queryForList(String sqlId, Map<String, Object> paramMap, RowMapper<T> rowMapper) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, paramMap, rowMapper);
	}

	@Override
	public <T> List<T> queryForList(String sqlId, Object param, RowMapper<T> rowMapper) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).queryForList(sqlId, param, rowMapper);
	}

	@Override
	public int execute(String sqlId, Map<String, Object> paramMap) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).execute(sqlId, paramMap);
	}

	@Override
	public Number execute4PrimaryKey(String sqlId, Map<String, Object> paramMap) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).execute4PrimaryKey(sqlId, paramMap);
	}

	@Override
	public int execute(String sqlId, Object param) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).execute(sqlId, param);
	}

	@Override
	public int[] batchUpdate(final String sqlId, Map<String, Object>[] batchValues) {

		final List<int[]> result = new ArrayList<int[]>();
		int arrayLength = 0;
		ExecutorService executor = newThreadPoolExecutor(Runtime.getRuntime().availableProcessors() + 1, "batchUpdate");
		ShardingBatchIsolator isolator = executeBatchParamterIsolate(sqlId, batchValues, executor);
		Set<Map.Entry<MappedSqlExecutor, List<Map<String, Object>>>> entrySet = isolator.getResources().entrySet();
		try {
			final StringBuffer exceptionStaktrace = new StringBuffer();
			for (final Map.Entry<MappedSqlExecutor, List<Map<String, Object>>> entry : entrySet) {
				try {
					MappedSqlExecutor mappedSqlExecutor = entry.getKey();
					@SuppressWarnings("unchecked")
					Map<String, Object>[] shardBatchParam = entry.getValue().toArray(new Map[entry.getValue().size()]);
					if (shardBatchParam.length > 0) {
						int[] aResult = mappedSqlExecutor.batchUpdate(sqlId, shardBatchParam);
						result.add(aResult);
						arrayLength += aResult.length;
					}

				} catch (Throwable t) {
					exceptionStaktrace.append(ExceptionUtils.getStackTrace(t));
				}
			}
			if (exceptionStaktrace.length() > 0) {
				throw new ConcurrencyFailureException(
						"unpected exception when execute the shard batch update, check previous log for details.\n"
								+ exceptionStaktrace);
			}
			int[] resultArray = new int[arrayLength];
			for (int i = 0; i < arrayLength;) {
				for (int j = 0; j < result.size(); j++) {
					for (int k = 0; k < result.get(j).length; k++) {
						resultArray[i++] = result.get(j)[k];
					}
				}
			}
			return resultArray;
		} finally {
			executor.shutdown();
		}

	}

	@Override
	public Map<String, Object> call(String sqlId, Map<String, Object> paramMap, List<SqlParameter> sqlParameters) {
		List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, paramMap);
		if (mappedSqlExecutor.size() != 1) {
			throwNotSupportMultShardException(sqlId);
		}
		return mappedSqlExecutor.get(0).call(sqlId, paramMap, sqlParameters);
	}

	/*-----------------------------------private method area---------------------------*/

	/**
	 * 功能描述：根据sqlId和参数对象查出MappedSqlExecutor集合<br>
	 * 输入参数：sqlId，参数对象<按照参数定义顺序> 
	 * @param sqlId，parameterObject
	 * 返回值:  list集合，元素是MappedSqlExecutor类型<说明> 
	 * @return List<MappedSqlExecutor>
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private List<MappedSqlExecutor> lookupMappedSqlExecutor(final String sqlId, final Object parameterObject) {
		List<MappedSqlExecutor> result = new ArrayList<MappedSqlExecutor>();
		// 根据sqlId先查找shardRouter
		ShardRouter shardRouter = ShardingConfigurationUtil.getStatementMappedShardRouter(configuration, sqlId);
		if (shardRouter == null) {
			// shardRouter为null，再根据namespace查找shardRouter
			String namespace = configuration.extractNamespace(sqlId);
			shardRouter = ShardingConfigurationUtil.getNamespaceMappedShardRouter(configuration, namespace);
		}
		if (shardRouter != null) {
			// 根据查找出的shardRouter，路由出shard
			String[] shards = shardRouter.processRoute(parameterObject);
			if (shards == null || shards.length == 0) {
				logger.warn(
						"The shard route to default shard because the ShardRouter[id='{}'] "
								+ "didn't return the shard result while execute by parameter:[{}]",
						shardRouter.getId(), parameterObject);
			} else {
				logger.debug("The statement[id='{}'] binded ShardRouter[id='{}'] which route shard{}",
						new Object[] { sqlId, shardRouter.getId(), Arrays.toString(shards) });
				for (String shard : shards) {
					MappedSqlExecutor mappedSqlExecutor = this.shardMappedSqlExectorMapping.get(shard);
					if (mappedSqlExecutor == null) {
						throw new DalException(
								"The shard name '" + shard + "' is strange, it is not registered in ShardRegister."
										+ "Please check the ShardRouter[id='" + shardRouter.getId() + "']");
					}
					result.add(mappedSqlExecutor);
				}
			}
		}
		// route to default shard.
		if (result.isEmpty()) {
			if (defualtShardName == null || "".equals(defualtShardName)) {
				throw new RouteException("Route no specified shard, And 'defualtShardName' is null");
			} else if (shardMappedSqlExectorMapping.get(defualtShardName) == null) {
				throw new RouteException("The defualtShardName name '" + defualtShardName
						+ "' is strange, it is not registered in ShardRegister.");
			}
			result.add(this.shardMappedSqlExectorMapping.get(this.defualtShardName));
		}
		return result;
	}

	/**
	 * 功能描述：抛出不支持多个shard 的异常<br>
	 * 输入参数：sqlId<按照参数定义顺序> 
	 * @param sqlId
	 * 返回值:  类型 <说明> 
	 * @return 返回值
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private void throwNotSupportMultShardException(String sqlId) {
		ShardRouter shardRouter = ShardingConfigurationUtil.getStatementMappedShardRouter(configuration, sqlId);
		throw new DalException("The operation is not supported execute simultaneously on multiple shard."
				+ "Please check the ShardRouter[id='" + shardRouter.getId() + "'],it can return multiple shard.");
	}

	private ShardingBatchIsolator executeBatchParamterIsolate(final String sqlId, Map<String, Object>[] batchValues,
			ExecutorService executor) {
		final ShardingBatchIsolator isolator = new ShardingBatchIsolator(this.shardMappedSqlExectorMapping.values());
		try {
			final CountDownLatch latch = new CountDownLatch(batchValues.length);
			final StringBuffer exceptionStaktrace = new StringBuffer();
			for (final Map<String, Object> param : batchValues) {
				try {
					List<MappedSqlExecutor> mappedSqlExecutor = lookupMappedSqlExecutor(sqlId, param);
					if (mappedSqlExecutor.size() != 1) {
						throwNotSupportMultShardException(sqlId);
					}
					isolator.emit(mappedSqlExecutor.get(0), param);
				} catch (Throwable t) {
					exceptionStaktrace.append(ExceptionUtils.getStackTrace(t));
				} finally {
					latch.countDown();
				}
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new ConcurrencyFailureException(
						"unexpected interruption when distribute the batch parameter to shard", e);
			}
			if (exceptionStaktrace.length() > 0) {
				throw new ConcurrencyFailureException(
						"unpected exception when distribute the batch parameter to shard, "
								+ "check previous log for details.\n" + exceptionStaktrace);
			}
			return isolator;
		} finally {
			// executor.shutdown();
		}
	}

	/**
	 * 功能描述：新建ThreadPoolExecutor
	 */
	private ExecutorService newThreadPoolExecutor(int poolSize, final String method) {
		int coreSize = Runtime.getRuntime().availableProcessors();
		if (poolSize < coreSize) {
			coreSize = poolSize;
		}
		ThreadFactory tf = new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "Thread created at ShardingDalClient method [" + method + "]");
				// t.setDaemon(true);
				return t;
			}
		};
		BlockingQueue<Runnable> queueToUse = new LinkedBlockingQueue<Runnable>();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(coreSize, poolSize, 60L, TimeUnit.SECONDS, queueToUse, tf,
				new ThreadPoolExecutor.CallerRunsPolicy());

		return executor;
	}

	public void afterPropertiesSet() throws Exception {

		Assert.notNull(shardRegister, "ShardRegister must not be null!");
		Assert.isTrue(!shardRegister.getDataSources().isEmpty(), "At least one shard registered in register!");

		if (isProfileLongTimeRunningSql()) {
			Assert.isTrue(longTimeRunningSqlIntervalThreshold > 0,
					"'longTimeRunningSqlIntervalThreshold' should have a positive value "
							+ "if 'profileLongTimeRunningSql' is set to true");
		}

		for (Shard shard : this.shardRegister.getShards()) {
			// String dbType =
			// DatabaseTypeProvider.getDatabaseType(shard.getDataSource());
			// logger.debug("this 'dataSource' database type is " + dbType);
			mappedSqlExecutor = new MappedSqlExecutor();
			mappedSqlExecutor.setConfiguration(configuration);
			mappedSqlExecutor.setDataSource(shard.getDataSource());
			mappedSqlExecutor.setLogPrefix(shard.getId());
			mappedSqlExecutor.setProfileLongTimeRunningSql(isProfileLongTimeRunningSql());
			mappedSqlExecutor.setLongTimeRunningSqlIntervalThreshold(longTimeRunningSqlIntervalThreshold);
			mappedSqlExecutor.setSqlAuditor(sqlAuditor);

			this.shardMappedSqlExectorMapping.put(shard.getId(), mappedSqlExecutor);
		}

		buildConfiguration();

	}

	/**
	 * 功能描述：读取配置文件把相关配置放入configuration对象<br>
	 * 输入参数：<按照参数定义顺序> 
	 * @param 参数说明
	 * 返回值:  类型 <说明> 
	 * @return 返回值
	 * @throw 异常描述
	 * @see 需要参见的其它内容
	 */
	private void buildConfiguration() {

		try {
			// find the entity class in class path.
			Set<Class<? extends Class<?>>> classSet = new HashSet<Class<? extends Class<?>>>();
			if (entityPackage != null && !"".equals(entityPackage)) {
				ResolverUtil<Class<?>> resolverUtil = new ResolverUtil<Class<?>>();
				resolverUtil.setClassLoader(getClass().getClassLoader());

				resolverUtil.find(new ResolverUtil.AnnotatedWith(Entity.class), entityPackage);
				classSet.addAll(resolverUtil.getClasses());

				resolverUtil.find(new ResolverUtil.AnnotatedWith(TableRoute.class), entityPackage);
				classSet.addAll(resolverUtil.getClasses());
			}
			// solve sqlMap configuration XML
			if (sqlMapConfigLocation != null) {
				for (Resource resource : sqlMapConfigLocation) {
					new XmlSqlMapBuilder(resource.getInputStream(), configuration, resource.getFilename()).parse();
				}
			}
			// solve Entity and CacheController Annotation
			for (Class<?> entityClass : classSet) {
				new AnnotationSqlMapBuilder(configuration, entityClass).parse();
				new AnnotationCacheBuilder(configuration, entityClass).parse();
			}
			// solve sharding configuration XML
			if (shardingConfigLocation != null) {
				for (Resource resource : shardingConfigLocation) {
					new XmlShardBuilder(resource.getInputStream(), configuration, resource.getFilename()).parse();
				}
				// solve ShardMapping Annotation
				for (Class<?> entityClass : classSet) {
					new AnnotationShardRefBuilder(configuration, entityClass).parse();
				}
			}

		} catch (ParsingException e) {
			logger.error(this.getClass() + "Error occurred.  Cause: ", e);
			throw e;
		} catch (IOException e) {
			throw new ParsingException("Error occurred.  Cause: ", e);
		}
	}

	protected void assertMapped(Object entity) {
		if (entity == null) {
			throw new DalException("the entity can't null");
		}
		Class<? extends Object> entityClass = entity.getClass();
		assertMapped(entityClass);
	}

	/**
	 * 功能描述：维护映射。<br>
	 * 根据实体类型查询configuration对象中是否有该实体类的mappedStatement对象。<br>
	 * 若有，则跳过；若没有，则扫描实体类，判断是否有TableRoute或Entity注解。<br>
	 * 若有，则日志告警配置的entityPackage下不包含此实体类；若没有，则报错。<br>
	 */
	protected void assertMapped(Class<?> entityClass) {
		if (entityClass == null) {
			throw new DalException("the entity can't null");
		}
		String sqlId = entityClass.getName() + ".insert";
		MappedStatement mappedStatement = configuration.getMappedStatement(sqlId);
		if (mappedStatement == null) {
			if (entityClass.isAnnotationPresent(TableRoute.class)) {
				logger.debug("Please configure the entityPackage for {} in order to it can scan the entity classes.",
						entityClass.getName());
				new AnnotationSqlMapBuilder(configuration, entityClass).parse();
			} else if (entityClass.isAnnotationPresent(Entity.class)) {
				logger.debug("Please configure the entityPackage for {} in order to it can scan the entity classes.",
						entityClass.getName());
				new AnnotationSqlMapBuilder(configuration, entityClass).parse();
			} else {
				throw new DalException("The persist method is not support this pojo:" + entityClass.getName());
			}

			if (entityClass.isAnnotationPresent(ShardMapping.class)) {
				logger.debug("Please configure the entityPackage for {} in order to it can scan the entity classes.",
						entityClass.getName());
				new AnnotationShardRefBuilder(configuration, entityClass).parse();
			}
			if (entityClass.isAnnotationPresent(CacheController.class)) {
				logger.debug("Please configure the entityPackage for {} in order to it can scan the entity classes.",
						entityClass.getName());
				new AnnotationCacheBuilder(configuration, entityClass).parse();
			}
		}
	}
}

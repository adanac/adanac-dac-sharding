package com.adanac.framework.dac.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.HeuristicCompletionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import com.adanac.framework.dac.datasource.ShardRegister;

/**
 * 功能描述：多数据源事务管理器<br>
 * 采用Best Effort 1PC Pattern的事务策略
 * @author adanac
 * @version 1.0
 */
public class MultiDataSourceTransactionManager implements PlatformTransactionManager, InitializingBean {

	private final Logger logger = LoggerFactory.getLogger(MultiDataSourceTransactionManager.class);

	private List<PlatformTransactionManager> transactionManagers;
	private ShardRegister shardRegister;

	public ShardRegister getShardRegister() {
		return shardRegister;
	}

	public void setShardRegister(ShardRegister shardRegister) {
		this.shardRegister = shardRegister;
	}

	@Override
	public MultiTransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {

		MultiTransactionStatus mts = new MultiTransactionStatus(transactionManagers.get(0));

		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager.initSynchronization();
			mts.setNewSynchonization();
		}

		try {

			for (PlatformTransactionManager transactionManager : transactionManagers) {
				mts.registerTransactionManager(definition, transactionManager);
			}

		} catch (Exception ex) {

			Map<PlatformTransactionManager, TransactionStatus> transactionStatuses = mts.getTransactionStatuses();

			for (PlatformTransactionManager transactionManager : transactionManagers) {
				try {
					if (transactionStatuses.get(transactionManager) != null) {
						transactionManager.rollback(transactionStatuses.get(transactionManager));
					}
				} catch (Exception ex2) {
					logger.warn("Rollback exception (" + transactionManager + ") " + ex2.getMessage(), ex2);
				}
			}

			if (mts.isNewSynchonization()) {
				TransactionSynchronizationManager.clear();
			}

			throw new CannotCreateTransactionException(ex.getMessage(), ex);
		}

		return mts;
	}

	/**
	 * 功能描述：提交操作<br>
	 */
	@Override
	public void commit(TransactionStatus status) throws TransactionException {

		MultiTransactionStatus multiTransactionStatus = (MultiTransactionStatus) status;

		boolean commit = true;
		Exception commitException = null;
		PlatformTransactionManager commitExceptionTransactionManager = null;

		for (PlatformTransactionManager transactionManager : reverse(transactionManagers)) {

			TransactionStatus transactionStatus = multiTransactionStatus.getTransactionStatus(transactionManager);
			if (commit) {
				try {
					transactionManager.commit(transactionStatus);
				} catch (Exception ex) {
					commit = false;
					commitException = ex;
					commitExceptionTransactionManager = transactionManager;
				}

			} else {

				// after unsucessfull commit we must try to rollback remaining
				// transaction managers
				try {
					transactionManager.rollback(transactionStatus);
				} catch (Exception ex) {
					logger.warn("Rollback exception (after commit) (" + transactionManager + ") " + ex.getMessage(),
							ex);
				}
			}
		}

		if (multiTransactionStatus.isNewSynchonization()) {
			TransactionSynchronizationManager.clear();
		}

		if (commitException != null) {
			boolean firstTransactionManagerFailed = commitExceptionTransactionManager == getLastTransactionManager();
			int transactionState = firstTransactionManagerFailed ? HeuristicCompletionException.STATE_ROLLED_BACK
					: HeuristicCompletionException.STATE_MIXED;
			throw new HeuristicCompletionException(transactionState, commitException);
		}
	}

	/**
	 * 功能描述：回滚操作<br>
	 */
	@Override
	public void rollback(TransactionStatus status) throws TransactionException {

		Exception rollbackException = null;
		PlatformTransactionManager rollbackExceptionTransactionManager = null;

		MultiTransactionStatus multiTransactionStatus = (MultiTransactionStatus) status;

		for (PlatformTransactionManager transactionManager : reverse(transactionManagers)) {
			TransactionStatus transactionStatus = multiTransactionStatus.getTransactionStatus(transactionManager);
			try {
				transactionManager.rollback(transactionStatus);
			} catch (Exception ex) {
				if (rollbackException == null) {
					rollbackException = ex;
					rollbackExceptionTransactionManager = transactionManager;
				} else {
					logger.warn("Rollback exception (" + transactionManager + ") " + ex.getMessage(), ex);
				}
			}
		}

		if (multiTransactionStatus.isNewSynchonization()) {
			TransactionSynchronizationManager.clear();
		}

		if (rollbackException != null) {
			throw new UnexpectedRollbackException("Rollback exception, originated at ("
					+ rollbackExceptionTransactionManager + ") " + rollbackException.getMessage(), rollbackException);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(shardRegister, "ShardRegister must not be null!");
		Assert.isTrue(!shardRegister.getDataSources().isEmpty(), "At least one shard registered in register!");
		transactionManagers = new ArrayList<PlatformTransactionManager>();
		for (DataSource dataSource : getShardRegister().getDataSources().values()) {
			PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
			transactionManagers.add(txManager);
		}

	}

	private <T> Iterable<T> reverse(Collection<T> collection) {

		List<T> list = new ArrayList<T>(collection);
		Collections.reverse(list);
		return list;
	}

	private PlatformTransactionManager getLastTransactionManager() {
		return transactionManagers.get(lastTransactionManagerIndex());
	}

	private int lastTransactionManagerIndex() {
		return transactionManagers.size() - 1;
	}
}

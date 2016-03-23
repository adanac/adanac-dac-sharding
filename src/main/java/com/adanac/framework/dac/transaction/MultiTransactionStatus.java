package com.adanac.framework.dac.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;

/**
 * 多事务状态
 * @author adanac
 * @version 1.0
 */
public class MultiTransactionStatus implements TransactionStatus {

	private final PlatformTransactionManager mainTransactionManager;
	private final Map<PlatformTransactionManager, TransactionStatus> transactionStatuses = Collections
			.synchronizedMap(new HashMap<PlatformTransactionManager, TransactionStatus>());

	private boolean newSynchonization;

	public MultiTransactionStatus(PlatformTransactionManager mainTransactionManager) {

		Assert.notNull(mainTransactionManager, "TransactionManager must not be null!");
		this.mainTransactionManager = mainTransactionManager;
	}

	public Map<PlatformTransactionManager, TransactionStatus> getTransactionStatuses() {
		return transactionStatuses;
	}

	public void setNewSynchonization() {
		this.newSynchonization = true;
	}

	public boolean isNewSynchonization() {
		return newSynchonization;
	}

	public void registerTransactionManager(TransactionDefinition definition,
			PlatformTransactionManager transactionManager) {
		getTransactionStatuses().put(transactionManager, transactionManager.getTransaction(definition));
	}

	public TransactionStatus getTransactionStatus(PlatformTransactionManager transactionManager) {
		return this.getTransactionStatuses().get(transactionManager);
	}

	@Override
	public boolean isRollbackOnly() {
		return getMainTransactionStatus().isRollbackOnly();
	}

	@Override
	public boolean isCompleted() {
		return getMainTransactionStatus().isCompleted();
	}

	@Override
	public boolean isNewTransaction() {
		return getMainTransactionStatus().isNewTransaction();
	}

	@Override
	public boolean hasSavepoint() {
		return getMainTransactionStatus().hasSavepoint();
	}

	@Override
	public void setRollbackOnly() {
		for (TransactionStatus ts : transactionStatuses.values()) {
			ts.setRollbackOnly();
		}
	}

	@Override
	public Object createSavepoint() throws TransactionException {

		SavePoints savePoints = new SavePoints();

		for (TransactionStatus transactionStatus : transactionStatuses.values()) {
			savePoints.save(transactionStatus);
		}
		return savePoints;
	}

	@Override
	public void rollbackToSavepoint(Object savepoint) throws TransactionException {
		SavePoints savePoints = (SavePoints) savepoint;
		savePoints.rollback();
	}

	@Override
	public void releaseSavepoint(Object savepoint) throws TransactionException {
		((SavePoints) savepoint).release();
	}

	@Override
	public void flush() {
		for (TransactionStatus transactionStatus : transactionStatuses.values()) {
			transactionStatus.flush();
		}
	}

	private TransactionStatus getMainTransactionStatus() {
		return transactionStatuses.get(mainTransactionManager);
	}

	/**
	 * 功能描述：保存点
	 * @author 作者 13092011
	 */
	private static class SavePoints {

		private final Map<TransactionStatus, Object> savepoints = new HashMap<TransactionStatus, Object>();

		private void addSavePoint(TransactionStatus status, Object savepoint) {

			Assert.notNull(status, "TransactionStatus must not be null!");
			this.savepoints.put(status, savepoint);
		}

		private void save(TransactionStatus transactionStatus) {
			Object savepoint = transactionStatus.createSavepoint();
			addSavePoint(transactionStatus, savepoint);
		}

		public void rollback() {
			for (TransactionStatus transactionStatus : savepoints.keySet()) {
				transactionStatus.rollbackToSavepoint(savepointFor(transactionStatus));
			}
		}

		private Object savepointFor(TransactionStatus transactionStatus) {
			return savepoints.get(transactionStatus);
		}

		public void release() {
			for (TransactionStatus transactionStatus : savepoints.keySet()) {
				transactionStatus.releaseSavepoint(savepointFor(transactionStatus));
			}
		}
	}
}

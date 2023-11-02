package com.kenshoo.integrations.service.impl;

import com.kenshoo.integrations.dao.IntegrationsDao;
import com.kenshoo.integrations.entity.Integration;
import com.kenshoo.integrations.exception.IntegrationPersistenceException;
import com.kenshoo.integrations.exception.KsIdNormalizationException;
import com.kenshoo.integrations.service.IntegrationsService;
import com.kenshoo.integrations.service.KsNormalizerClient;
import com.kenshoo.integrations.service.LoggerService;

import java.io.IOException;
import java.util.List;

public class IntegrationsServiceImpl implements IntegrationsService {

	private static final int ROWS_COUNT_PER_INSERT = 1;
	private static final int ROWS_COUNT_PER_UPDATE = 1;

	private final KsNormalizerClient ksNormalizerClient;
	private final IntegrationsDao integrationsDao;
	private final LoggerService loggerService;

	public IntegrationsServiceImpl(final KsNormalizerClient ksNormalizerClient, final IntegrationsDao integrationsDao,
	                               final LoggerService loggerService) {
		this.ksNormalizerClient = ksNormalizerClient;
		this.integrationsDao = integrationsDao;
		this.loggerService = loggerService;
	}

	@Override
	public void insertIntegration(final String ksId, final String data) {
		final String normalizedKsId = normalizeKsId(ksId);

		insertIntegrationToDB(normalizedKsId, data);
	}

	private String normalizeKsId(final String ksId) {
		try {
			return ksNormalizerClient.normalize(ksId);
		} catch (IOException e) {
			loggerService.error("An error occurred during ks id normalization: " + ksId, e);

			throw new KsIdNormalizationException(ksId, e);
		}
	}

	private void insertIntegrationToDB(final String normalizedKsId, final String data) {
		final int insertedRows = integrationsDao.insert(normalizedKsId, data);

		if (insertedRows != ROWS_COUNT_PER_INSERT) {
			throw new IntegrationPersistenceException("Unexpected number of rows affected by insert: " + insertedRows);
		}
	}

	@Override
	public List<Integration> fetchIntegrationsByKsId(final String ksId) {
		final String normalizeKsId = normalizeKsId(ksId);

		return integrationsDao.fetchByKsId(normalizeKsId);
	}

	@Override
	public int migrate() {
		final List<Integration> integrations = integrationsDao.fetchAll();
		int affectedRows = 0;

		for (final Integration integration : integrations) {
			final String ksId = integration.getKsId();

			affectedRows += updateIfNotNormalized(ksId);
		}

		return affectedRows;
	}


	private int updateIfNotNormalized(final String ksId) {
		int updatedRows = 0;

		final String normalizedKsId = normalizeKsId(ksId);
		if (isNotNormalized(ksId, normalizedKsId)) {
			updatedRows = updateKsIdInDB(ksId, normalizedKsId);
		}

		return updatedRows;
	}

	private static boolean isNotNormalized(final String ksId, final String normalizedKsId) {
		return !ksId.equals(normalizedKsId);
	}

	private int updateKsIdInDB(final String ksId, final String normalizedKsId) {
		final int updatedRows = integrationsDao.updateKsId(ksId, normalizedKsId);

		if (updatedRows != ROWS_COUNT_PER_UPDATE) {
			throw new IntegrationPersistenceException("Unexpected number of rows affected by update: " + updatedRows);
		}

		return updatedRows;
	}
}

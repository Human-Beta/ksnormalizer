package com.kenshoo.integrations.exception;

public class KsIdNormalizationException extends RuntimeException {
	public KsIdNormalizationException(final String ksId, final Throwable cause) {
		super("Failed to normalize ksId: " + ksId, cause);
	}
}

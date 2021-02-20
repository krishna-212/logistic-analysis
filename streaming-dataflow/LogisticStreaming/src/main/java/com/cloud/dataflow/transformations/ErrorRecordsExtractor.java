package com.cloud.dataflow.transformations;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Gets records when insertion to BigQuery fails
 */
public class ErrorRecordsExtractor extends DoFn<BigQueryInsertError, String> {

	static ObjectMapper mapper = new ObjectMapper();
	private static final long serialVersionUID = 2980389650251142298L;

	static final Logger LOG = LoggerFactory.getLogger(ErrorRecordsExtractor.class);

	@ProcessElement
	public void processElement(ProcessContext c) {
		try {
			LOG.error(String.valueOf(c.element().getError()));
			c.output(mapper.writeValueAsString(c.element().getRow()));
		} catch (JsonProcessingException e) {
			LOG.error(String.format("TableRow not in parseable format: %s", e.getMessage()));
		}
	}

}

package com.cloud.dataflow.realtime.transformations;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloud.dataflow.realtime.exceptions.InvalidDataException;
import com.cloud.dataflow.realtime.util.Constants;
import com.cloud.dataflow.realtime.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class InputDataProcessing extends DoFn<String, TableRow> {

	public static final Logger LOG = LoggerFactory.getLogger(InputDataProcessing.class);

	private static final long serialVersionUID = -4769014801616433682L;

	TupleTag<TableRow> bigqueryRecords;
	TupleTag<String> deadLetterTag;

	public InputDataProcessing(TupleTag<TableRow> bigqueryRecords, TupleTag<String> deadLetterTag) {
		this.bigqueryRecords = bigqueryRecords;
		this.deadLetterTag = deadLetterTag;
	}

	/*
	 * PTransform process input stream record and load success record to BQ and put
	 * failure record in Dead letter Tag
	 */
	@ProcessElement
	public void processElement(ProcessContext context) {
		try {
			// Avoid processing header records
			if (!context.element().startsWith(Constants.EVENT_DATE)) {
				List<String> splitPayload = Utility.processPayload(context.element());

				if (splitPayload.size() != Constants.COLUMN_LENGTH)
					throw new InvalidDataException(
							String.format("%s:%s", "CSV length doesn't match with the schema", context.element()));

				// Converting Records to TableRow
				TableRow outputTableRow = Utility.convertToTableRow(splitPayload);

				context.output(bigqueryRecords, outputTableRow);

			} else {
				LOG.debug(String.format("Header:%s", context.element()));
			}
		} catch (InvalidDataException ie) {
			LOG.error(ie.toString());
			context.output(deadLetterTag, context.element());
		} catch (Exception e) {
			LOG.error(e.toString());
			context.output(deadLetterTag, context.element());
		}

	}

}

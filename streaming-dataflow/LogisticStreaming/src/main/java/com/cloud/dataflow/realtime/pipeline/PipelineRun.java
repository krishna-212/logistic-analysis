package com.cloud.dataflow.realtime.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

import com.cloud.dataflow.realtime.options.RealtimeTableOptions;
import com.cloud.dataflow.realtime.transformations.ErrorRecordsExtractor;
import com.cloud.dataflow.realtime.transformations.InputDataProcessing;
import com.google.api.services.bigquery.model.TableRow;

public class PipelineRun {

	static TupleTag<String> deadLetterTag = new TupleTag<String>("deadLetterTag") {
		private static final long serialVersionUID = 4807907023563534805L;

	};

	static TupleTag<TableRow> bigqueryRecords = new TupleTag<TableRow>("bigqueryRecords") {

		private static final long serialVersionUID = -1196119824437003623L;

	};

	protected static PipelineResult run(RealtimeTableOptions pipelineOptions) {

		Pipeline pipeline = Pipeline.create(pipelineOptions);

		// Stream data from GCS and watch for new files in every 5 minutes
		PCollection<String> inputData = pipeline.apply("Stream data from GCS",
				TextIO.read().from(pipelineOptions.getFilePath())
						.watchForNewFiles(Duration.standardMinutes(5), Watch.Growth.never())
						.withHintMatchesManyFiles());

		// Processing Streaming Records
		PCollectionTuple resultSet = inputData.apply("Processing Streaming Data",
				ParDo.of(new InputDataProcessing(bigqueryRecords, deadLetterTag)).withOutputTags(bigqueryRecords,
						TupleTagList.of(deadLetterTag)));

		// Write success records to BQ
		WriteResult bqWriteResults = resultSet.get(bigqueryRecords).apply("BQ SteamingInserts",
				BigQueryIO.writeTableRows().withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS).optimizedWrites()
						.to(pipelineOptions.getTableName())
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
						.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()).withExtendedErrorInfo());

		// Logging Failure reason and writing Fail record to BQ dead letter queue
		PCollection<String> failureBQResult = bqWriteResults.getFailedInsertsWithErr().apply("Format failed BQ records",
				ParDo.of(new ErrorRecordsExtractor()));

		// Combing record from dead letter and BQ failed result
		PCollection<String> finalErrorRecords = PCollectionList.of(failureBQResult).and(resultSet.get(deadLetterTag))
				.apply("Flatten Dead letter tag and BQ failed record", Flatten.<String>pCollections());

		// Micro-batching and writing it to GCS as GCS doesn't support stream write
		finalErrorRecords
				.apply("Applying windowing to write failed record to GCS",
						Window.<String>into(FixedWindows.of(Duration.standardHours(1)))
								.withAllowedLateness(Duration.standardMinutes(1))
								.triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
										.plusDelayOf(Duration.standardMinutes(3))))
								.discardingFiredPanes())
				.apply("Write failed record to GCS",
						TextIO.write().withWindowedWrites().to(pipelineOptions.getErrorBucket()).withNumShards(1));

		PipelineResult pipelineResult = pipeline.run();

		return pipelineResult;
	}
}

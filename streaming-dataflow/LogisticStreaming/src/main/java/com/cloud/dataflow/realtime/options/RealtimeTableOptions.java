package com.cloud.dataflow.realtime.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.Description;

public interface RealtimeTableOptions extends DataflowPipelineOptions, DataflowWorkerHarnessOptions {

	@Description("BigQuery Table Name")
	String getTableName();

	void setTableName(String tableName);

	@Description("Input File Path")
	String getFilePath();

	void setFilePath(String filePath);

	@Description("Error storage location")
	String getErrorBucket();

	void setErrorBucket(String errorBucket);

}

package com.cloud.dataflow.realtime.pipeline;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloud.dataflow.realtime.options.RealtimeTableOptions;

public class StreamingPipeline {

	/**
	 * Entry point to framework. Starts the Dataflow job to stream file from GCS and
	 * write success records to BQ in streaming mode. In case of failure during
	 * transformation stage, put that record in dead letter queue and store it in
	 * GCS
	 * 
	 * @param args
	 */

	static final Logger LOG = LoggerFactory.getLogger(StreamingPipeline.class);

	public static void main(String[] args) {

		RealtimeTableOptions pipelineOptions = getPipelineOptions(args);

		pipelineOptions.setWorkerMachineType("n1-standard-4");
		pipelineOptions.setNumberOfWorkerHarnessThreads(32);

		PipelineRun.run(pipelineOptions);

	}

	/**
	 * Accepts main class arguments and creates {@link RealtimeTableOptions}
	 *
	 * @param args
	 * @return
	 */
	public static RealtimeTableOptions getPipelineOptions(final String args[]) {
		PipelineOptionsFactory.register(RealtimeTableOptions.class);
		return PipelineOptionsFactory.fromArgs(args).as(RealtimeTableOptions.class);
	}
}

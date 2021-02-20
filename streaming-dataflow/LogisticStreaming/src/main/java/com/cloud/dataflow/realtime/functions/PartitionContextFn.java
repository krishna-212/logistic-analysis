package com.cloud.dataflow.realtime.functions;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class PartitionContextFn implements SerializableFunction<String, String> {

	private static final long serialVersionUID = -4660600821782082913L;
	ValueProvider<String> bucketName;

	public PartitionContextFn(ValueProvider<String> bucketName) {
		this.bucketName = bucketName;
	}

	@Override
	public String apply(String inputMessage) {
		return String.format("%s/", bucketName.get());
	}
}

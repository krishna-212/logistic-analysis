package com.cloud.dataflow.realtime.functions;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

public class DynamicDatasetFileRenaming implements FileIO.Write.FileNaming {

	static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private static final long serialVersionUID = 1560063588163106153L;
	String datasetTableName;

	public DynamicDatasetFileRenaming(String datasetTableName) {
		this.datasetTableName = datasetTableName;
	}

	@Override
	public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
			Compression compression) {
		Date currentDate = new Date(window.maxTimestamp().getMillis());
		String fullFilePath = String.format("gs://%s/%s/%s-%s-%s-%s", datasetTableName, formatter.format(currentDate),
				window.maxTimestamp().getMillis(), pane.getIndex(), numShards, shardIndex);

		return fullFilePath;
	}

}

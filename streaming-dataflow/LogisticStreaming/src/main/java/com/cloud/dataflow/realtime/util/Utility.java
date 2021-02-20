package com.cloud.dataflow.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.google.api.services.bigquery.model.TableRow;

public class Utility {

	/*
	 * Splitting input records on the basis of regex. Regex fails something of stack
	 * overflow error in case of multiple recursive code running together So wrote
	 * custom split operator which is light weight code
	 */
	public static List<String> processPayload(String input) {

		// Removing Special character
		String removecontrolChar = input.replaceAll("\\p{Cc}", " ").replaceAll("\\s{2,}", " ");
		List<String> result = new ArrayList<>();
		AtomicInteger start = new AtomicInteger(0);
		AtomicBoolean inQuotes = new AtomicBoolean(false);

		// Avoid commas inside double quotes
		IntStream.range(0, removecontrolChar.length()).forEach(current -> {
			if (removecontrolChar.charAt(current) == '\"')
				inQuotes.set(!inQuotes.get());
			else if (removecontrolChar.charAt(current) == ',' && !inQuotes.get()) {
				result.add(removecontrolChar.substring(start.get(), current));
				start.set(current + 1);
			}
		});
		result.add(removecontrolChar.substring(start.get()));
		return result;
	}

	/* Converting list to TableRow */
	public static TableRow convertToTableRow(List<String> splitPayload) throws ParseException {
		TableRow output = new TableRow();
		isTimeStampValid(splitPayload.get(0));
		output.set(Constants.EVENT_DATE, splitPayload.get(0));
		output.set(Constants.PAYLOAD, splitPayload.get(1));
		return output;
	}

	/*
	 * Checking if the time is valid
	 */
	public static boolean isTimeStampValid(String timestamp) throws ParseException {

		SimpleDateFormat format = new SimpleDateFormat(Constants.TIMEFORMAT);
		format.setLenient(true);
		format.parse(timestamp);
		return true;
	}
}

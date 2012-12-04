package com.mapr.util;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.*;
import java.util.*;
import java.io.*;

public class FixedWidthLoader extends LoadFunc
{

	/** Describes a single field (character range)
	 */
	private class FWField {
		int start;
		int end;

		FWField(int character) {
			this.start = character;
			this.end = character+1;
		}
		
		FWField(int start, int end) {
			this.start = start;
			this.end = end;
		}

	}
	
	/* Holds descriptors for fields to be parsed */
	List<FWField> fields = new ArrayList<FWField>();
	private RecordReader<LongWritable, Text> reader;

	/** Construct a FixedWidthLoader using the specified mappings.  
     *  The fields parameter is a string similar to that used in the unix 'cut' command.
	 *  e.g. -2,3-10,20-30,100,102-
	 *  Offsets specified in this string are 0-based. Note that the string "-" should result in the 
	 *  same output as a standard TextLoader.
	 */
	public FixedWidthLoader(String... fields) {
		for(String range : fields) {
			if(range.indexOf("-") != -1) {
				/* Actual range */
				int start, end;
				String[] range_offsets = range.split("-", 2);
				if(range_offsets[0].equals("")) {
					start=0;
				} else {
					start=Integer.parseInt(range_offsets[0]);
				}

				if(range_offsets[1].equals("")) {
					end=Integer.MAX_VALUE;
				} else {
					end=Integer.parseInt(range_offsets[1]);
				}
				this.fields.add(new FWField(start,end));	
			} else {
				/* Single char */
				int f_offset = Integer.parseInt(range);
				this.fields.add(new FWField(f_offset));
			}
		}

	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public void setLocation(String location, Job job) throws IOException
	{
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
	{
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple output_rec = TupleFactory.getInstance().newTuple(this.fields.size());
		try {
			if(this.reader.nextKeyValue()) {
				Text rawText = (Text)reader.getCurrentValue();
				String input_rec = rawText.toString();

				for(int i=0;i<fields.size();i++) {
					FWField field = fields.get(i);
					/* Ignore any field that starts after the end of the record */
					if(field.start > input_rec.length()) {
						continue;
					}

					String extractedField = input_rec.substring(field.start, Math.min(field.end, input_rec.length()));
					output_rec.set(i, extractedField);
					
				}
				return output_rec;
			}
		} catch ( Exception e) {
			throw new IOException("An error was encountered while parsing a record", e);
		}
		return null;
	}

}

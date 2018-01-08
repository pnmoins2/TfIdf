package cs.Lab2.WordCount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable totalWordCount = new IntWritable();
	
    	// Overriding of the reduce function
	// Input : ([word, docId], List of subWordCounts)
	// Output : ([word, docId], wordCount)
   	@Override
    	protected void reduce(final Text cleI, final Iterable<IntWritable> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		int sum = 0;

		// We sum all the sub wordCounts
		Iterator<IntWritable> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			sum += iterator.next().get();
		}

		// Output
		totalWordCount.set(sum);
		context.write(cleI,totalWordCount);
    	}

}

package cs.Lab2.Sort;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortReducer extends Reducer<FloatWritable,Text,Text,FloatWritable> {
	private Text docIdAndWord = new Text();
	private FloatWritable tfIdf = new FloatWritable();
	
    // Overriding of the reduce function
	// Input : (-tfIdf, List of [word, docId])
	// Output : ([word, docId], tfIdf)
	@Override
    protected void reduce(final FloatWritable cleI, final Iterable<Text> listevalI, final Context context) throws IOException,InterruptedException
    {
    	// Recover the tfIdf
    	tfIdf.set(-cleI.get());
    	
        // For each [word, docId]
        Iterator<Text> iterator = listevalI.iterator();
        while (iterator.hasNext())
        {
        	// Recover the [docId, word]
        	docIdAndWord.set(iterator.next().toString());
        	
        	// Output
        	context.write(docIdAndWord, tfIdf);
        }
    }
}

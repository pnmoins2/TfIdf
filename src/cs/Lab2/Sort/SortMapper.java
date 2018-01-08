package cs.Lab2.Sort;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Text, Text, FloatWritable, Text> {
	private FloatWritable minusTfIdf = new FloatWritable();
	
	// Overriding of the map method
	// Input : ([word, docId], tfIdf)
	// Output : (-tfIdf, [word, docId])
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    {		
		// Recover the tfIdf
		float tfIdf = Float.parseFloat(valE.toString());
		
		minusTfIdf.set(-tfIdf);
		context.write(minusTfIdf, keyE);
    }
}
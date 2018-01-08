package cs.Lab2.WordCount;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable integer = new IntWritable();
	private JSONObject docIdAndWordObject = new JSONObject();
	private Text docIdAndWordText = new Text();
	
	// Overriding of the map method
	// Input : (lineNumber, lineOfText)
	// Output : ([word, docId], wordCountOnLine)
	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    	{	
		// Recover the line
		String line = valE.toString();

		// Split the line
		StringTokenizer tokenizer = new StringTokenizer(line);

		// Initiate the hash map [word --> wordCount]
		Map<String, Integer> h = new HashMap<>();

		// For each word of the line
		while (tokenizer.hasMoreTokens())
		{
			// Recover the current word
			String currentWord = tokenizer.nextToken();

			String regex = "(\\.|-|,|;|\\!|\\?|\'|\"|\\\\|\\(|\\))+";
			currentWord = currentWord.replaceAll("^"+regex,"");
			currentWord = currentWord.replaceAll(regex+"$","");

			// Increase the wordCount in the hash map
			if (!currentWord.isEmpty())
			{
				if (h.containsKey(currentWord))
				{
					h.put(currentWord, h.get(currentWord) + 1);
				}
				else
				{
					h.put(currentWord, 1);
				}
			}
		}
        
		// For each element of the hash map
		java.util.Set<Entry<String, Integer>> setH = h.entrySet();
		Iterator<Entry<String, Integer>> iterator = setH.iterator();
		while(iterator.hasNext()){
			// Recover the docId
			FileSplit split = (FileSplit) context.getInputSplit();
			String[] filenameSplit = split.getPath().toString().split("/");
			String filename = filenameSplit[filenameSplit.length -1];

			// Recover the current [word --> wordCount]
			Entry<String, Integer> e = iterator.next();

			// Output Key
			docIdAndWordObject.put("docId", filename);
			docIdAndWordObject.put("word", e.getKey());

			// Convert the JSON Object to a JSON string
			docIdAndWordText.set(docIdAndWordObject.toJSONString());

			// Output Value
			integer.set(e.getValue());

			// Output
			context.write(docIdAndWordText, integer);
		}
    	}
}

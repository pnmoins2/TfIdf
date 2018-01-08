package cs.Lab2.TfIdf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TfIdfReducer extends Reducer<Text,Text,Text,FloatWritable> {
	private int numberOfDocuments = 1;
	private JSONParser parser = new JSONParser();
	private JSONObject wordAndDocIdObject = new JSONObject();
	private Text wordAndDocIdText = new Text();
	private FloatWritable tfIdf = new FloatWritable();
	
	// Recover the number of documents
	public void setup (Context context) {
		numberOfDocuments = context.getConfiguration().getInt("numberOfDocuments", 1);
	}
	
    	// Overriding of the reduce function
	// Input : (word, List of [docId, wordCount, wordsPerDoc])
	// Output : ([word, docId], tfIdf)
    	@SuppressWarnings("unchecked")
	@Override
    	protected void reduce(final Text cleI, final Iterable<Text> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		int docsPerWord = 0;
		JSONObject docIdWordCountAndWordsPerDocObject;
		String docIdWordCountAndWordsPerDocString;
		List<JSONObject> cache = new ArrayList<JSONObject>();

		// 1st step : Count the documents with a word

		// For each document
		Iterator<Text> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// Recover the JSON string
			docIdWordCountAndWordsPerDocString = iterator.next().toString();

			try {
				// Convert the JSON string to a JSON object
				docIdWordCountAndWordsPerDocObject = (JSONObject) this.parser.parse(docIdWordCountAndWordsPerDocString);

				// Add the JSON object to a list for the next step
				cache.add(docIdWordCountAndWordsPerDocObject);

				// Count the number of documents with a word
				docsPerWord += 1;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Initiate the local variables for the second step
		String docId;
		Long wordCount;
		Long wordsPerDoc;

		// For each document 
		for(JSONObject valI : cache)
		{
			// Recover the information related to the word and the document
			docId = (String) valI.get("docId");
			wordCount = (Long) valI.get("wordCount");
			wordsPerDoc = (Long) valI.get("wordsPerDoc");

			// Output key
			wordAndDocIdObject.put("docId",docId);
			wordAndDocIdObject.put("word", cleI.toString());

			// Convert the JSON object to a JSON string
			wordAndDocIdText.set(wordAndDocIdObject.toJSONString());

			// Compute the tfIdf for [word, document]
			tfIdf.set((float) ((float)wordCount / wordsPerDoc * Math.log((float)numberOfDocuments / docsPerWord)));

			// Output
			context.write(wordAndDocIdText,tfIdf);
		}
    	}
}

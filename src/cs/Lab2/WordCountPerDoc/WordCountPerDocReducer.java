package cs.Lab2.WordCountPerDoc;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordCountPerDocReducer extends Reducer<Text,Text,Text,Text> {
	private JSONParser parser = new JSONParser();
	private JSONObject wordAndDocIdObject = new JSONObject();
	private Text wordAndDocIdText = new Text();
	private JSONObject wordCountAndWordsPerDocObject = new JSONObject();
	private Text wordCountAndWordsPerDocText = new Text();
	
    	// Overriding of the reduce function
	// Input : (docId, list of [word, wordCount])
	// Output : ([word, docId], [wordCount, wordsPerDoc])
   	@SuppressWarnings("unchecked")
	@Override
    	protected void reduce(final Text cleI, final Iterable<Text> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		Long wordsPerDoc = new Long(0);
		JSONObject wordAndWordCountObject;
		String wordAndWordCountText;
		String word;
		Long wordCount;
		List<JSONObject> cache = new ArrayList<JSONObject>();

		// For each word
		Iterator<Text> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// Recover the JSON string
			wordAndWordCountText = iterator.next().toString();

			try {
				// Convert the JSON string to a JSON object
				wordAndWordCountObject = (JSONObject) this.parser.parse(wordAndWordCountText);

				// Add the JSON object to a list for the next step
				cache.add(wordAndWordCountObject);

				// Recover the wordCount related to the current word
				wordCount =  (Long) wordAndWordCountObject.get("wordCount");

				// Count the total number of word in the document
				wordsPerDoc += wordCount;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// For each word
		for(JSONObject valI : cache)
		{
			// Recover the information within the JSON
			word = (String) valI.get("word");
			wordCount = (Long) valI.get("wordCount");

			// Output key
			wordAndDocIdObject.put("docId",cleI.toString());
			wordAndDocIdObject.put("word", word);

			// Output value
			wordCountAndWordsPerDocObject.put("wordCount", wordCount);
			wordCountAndWordsPerDocObject.put("wordsPerDoc", wordsPerDoc);

			// Convert the JSON objects to JSON strings
			wordAndDocIdText.set(wordAndDocIdObject.toJSONString());
			wordCountAndWordsPerDocText.set(wordCountAndWordsPerDocObject.toJSONString());

			// Output
			context.write(wordAndDocIdText,wordCountAndWordsPerDocText);
		}
    	}
}

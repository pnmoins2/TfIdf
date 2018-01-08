package cs.Lab2.WordCountPerDoc;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class WordCountPerDocMapper extends Mapper<Text, Text, Text, Text> {
	private JSONParser parser = new JSONParser();
	private Text docIdText = new Text();
	private JSONObject wordAndWordCountObject = new JSONObject();
	private Text wordAndWordCountText = new Text();
	
	// Overriding of the map method
	// Input : ([word, docId], wordCount)
	// Output : (word, [docId, wordCount])
	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
   	{
		// Initiate the local variables
		Long wordCount = Long.parseLong(valE.toString());
		JSONObject docIdAndWord;

		try {
			// Convert the JSON string to a JSON object
			docIdAndWord = (JSONObject) this.parser.parse(keyE.toString());

			// Recover the information within the JSON
			String docIdString = (String) docIdAndWord.get("docId");
			String word = (String) docIdAndWord.get("word");

			// Output value
			wordAndWordCountObject.put("word", word);
			wordAndWordCountObject.put("wordCount", wordCount);

			// Convert the JSON object to a JSON string
			wordAndWordCountText.set(wordAndWordCountObject.toJSONString());

			// Output key
			docIdText.set(docIdString);

			// Output
			context.write(docIdText, wordAndWordCountText);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	}
}

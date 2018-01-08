package cs.Lab2.TfIdf;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class TfIdfMapper extends Mapper<Text, Text, Text, Text> {
	private JSONParser parser = new JSONParser();
	private Text wordText = new Text();
	private JSONObject docIdWordCountAndWordsPerDocObject = new JSONObject();
	private Text docIdWordCountAndWordsPerDocText = new Text();
	
	// Overriding of the map method
	// Input : ([word, docId], [wordCount, wordsPerDoc])
	// Output : (word, [docId, wordCount, wordsPerDoc])
	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text keyE, Text valE, Context context) throws IOException,InterruptedException
    {		
		// Initiate the local variables
        JSONObject docIdAndWord;
        JSONObject wordCountAndWordsPerDoc;
        
        try {
        	// Convert the JSON strings to JSON objects
	        docIdAndWord = (JSONObject) this.parser.parse(keyE.toString());
	        wordCountAndWordsPerDoc = (JSONObject) this.parser.parse(valE.toString());
	        
	        // Recover the different elements
	        String docId = (String) docIdAndWord.get("docId");
	        String wordString = (String) docIdAndWord.get("word");
	        Long wordCount = (Long) wordCountAndWordsPerDoc.get("wordCount");
	        Long wordsPerDoc = (Long) wordCountAndWordsPerDoc.get("wordsPerDoc");
	        
	        // Output Key
	        wordText.set(wordString);
	        
	        // Output Value
	        docIdWordCountAndWordsPerDocObject.put("docId", docId);
	        docIdWordCountAndWordsPerDocObject.put("wordCount", wordCount);
	        docIdWordCountAndWordsPerDocObject.put("wordsPerDoc", wordsPerDoc);
	        
	        // Convert the JSON output to a JSON string
	        docIdWordCountAndWordsPerDocText.set(docIdWordCountAndWordsPerDocObject.toJSONString());
	        
	        // Output
	        context.write(wordText, docIdWordCountAndWordsPerDocText);
        } catch (ParseException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
}
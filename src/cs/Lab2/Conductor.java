package cs.Lab2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cs.Lab2.WordCount.WordCount;
import cs.Lab2.WordCountPerDoc.MatrixBuilding;
import cs.Lab2.Sort.Sort;
import cs.Lab2.TfIdf.TfIdf;

public class Conductor {
	private static String wordCountOutput = "/1_WordCount";
	private static String wordCountPerDocOutput = "/2_WordCountPerDoc";
	private static String tfIdfOutput = "/3_TfIdf";
	private static String sortOutput = "/4_Sort";
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {

        		System.out.println("Usage: [input] [output]");

            		System.exit(-1);

        	}
		
		FileSystem fs = FileSystem.newInstance(conf);
		
		// Recover the global parameters
		int numberOfDocuments = fs.listStatus(new Path(args[0])).length;
		conf.setInt("numberOfDocuments", numberOfDocuments);
		
		String globalOutput = args[1];
		
		// First Step : WordCount
		WordCount wordCountDriver = new WordCount(conf);
		String[] wordCountArgs = new String[2];
		wordCountArgs[0] = args[0];
		wordCountArgs[1] = globalOutput + wordCountOutput;
		wordCountDriver.run(wordCountArgs);
		
		// Second Step : WordCountPerDoc
		MatrixBuilding wordCountPerDocDriver = new MatrixBuilding(conf);
		String[] wordCountPerDocArgs = new String[2];
		wordCountPerDocArgs[0] = wordCountArgs[1];
		wordCountPerDocArgs[1] = globalOutput + wordCountPerDocOutput;
		wordCountPerDocDriver.run(wordCountPerDocArgs);
		
		// ThirdStep : TfIdf
		TfIdf tfIdfDriver = new TfIdf(conf);
		String[] tfIdfArgs = new String[2];
		tfIdfArgs[0] = wordCountPerDocArgs[1];
		tfIdfArgs[1] = globalOutput + tfIdfOutput;
		tfIdfDriver.run(tfIdfArgs);
		
		// Fourth Step : Sort TfIdf
		Sort sortDriver = new Sort(conf);
		String[] sortArgs = new String[2];
		sortArgs[0] = tfIdfArgs[1];
		sortArgs[1] = globalOutput + sortOutput;
		sortDriver.run(sortArgs);
		
		// Fifth Step : Display the first 20 values
		// Recover the Output  Location
		String outputFile = sortArgs[1] + "/part-r-00000";
		Path path = new Path(outputFile);
		// Open the file
		FSDataInputStream in = fs.open(path);
		// Line Counter
		int numberLines = 0;
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			System.out.println("---- 20 words with the highest tfIdf ----");
			
			while (numberLines < 20 && line !=null){				
				// Print Line
				System.out.println(line);
				
				// Count the line
				numberLines += 1;
				
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
	}

}

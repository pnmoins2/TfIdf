package cs.Lab2.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordCount extends Configured implements Tool {
	private Configuration conf;
	
	public WordCount(Configuration conf)
	{
		this.conf = conf;
	}
	
    public int run(String[] args) throws Exception {
    	if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }
    	
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(conf);

        job.setJobName("WordCount");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        Path inputFilePath = new Path(args[0]);
    	FileInputFormat.addInputPath(job, inputFilePath);
    	
        Path outputFilePath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputFilePath);


        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(conf);

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }


        return job.waitForCompletion(true) ? 0: 1;

    }


    public static void main(String[] args) throws Exception {
    	Configuration config = new Configuration();
    	
        WordCount wordCountDriver = new WordCount(config);
        
        int res = ToolRunner.run(wordCountDriver, args);

        System.exit(res);
    }

}

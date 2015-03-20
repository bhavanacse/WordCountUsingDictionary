package wordcountwithdictionary;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WCWD_Driver {
	
	private static Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) throws URISyntaxException {
		System.out.println ("Provide the input folder name: ");
		String inputFolderName = scanner.nextLine();
		
		System.out.println ("Provide the output folder name: ");
		String outputFolderName = scanner.nextLine();
		
		System.out.println ("Provide the ID Collection file name: ");
		String  IDCollectionFileName = scanner.nextLine();
		
		System.out.println ("Provide the Vocabulary file name: ");
		String vocabFileName = scanner.nextLine();
		
		System.out.println ("Your MapReduce task for " + IDCollectionFileName + " has started......." );
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(wordcountwithdictionary.WCWD_Driver.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		String vocabFilePath = "/user/idcuser/" + inputFolderName + "/" + vocabFileName;
		
		DistributedCache.addCacheFile(new URI(vocabFilePath), conf);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("/user/idcuser/" + inputFolderName + "/" + IDCollectionFileName));
	    FileOutputFormat.setOutputPath(conf, new Path("/user/idcuser/" + outputFolderName + "/"));

		conf.setMapperClass(wordcountwithdictionary.WCWD_Mapper.class);
        conf.setCombinerClass(wordcountwithdictionary.WCWD_Reducer.class);
		conf.setReducerClass(wordcountwithdictionary.WCWD_Reducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

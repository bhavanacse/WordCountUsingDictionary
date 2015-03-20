package wordcountwithdictionary;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WCWD_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final IntWritable wordCount = new IntWritable();
	private IntWritable docID = new IntWritable();
	private IntWritable wordID = new IntWritable();
	private Text word = new Text();
	private Path[] localDictionaryFile;
	private BufferedReader cacheReader;
	private HashMap<IntWritable, String> textFileMap = new HashMap<IntWritable, String>();
	
	
	public void configure(JobConf jobConf) {
        // Get the cached archives/files
        try {
			localDictionaryFile = DistributedCache.getLocalCacheFiles(jobConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
     // TODO Auto-generated method stub
	if(localDictionaryFile != null && localDictionaryFile.length > 0){
    	try {
			cacheReader = new BufferedReader(new FileReader(localDictionaryFile[0].toString()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	String textLine;
    	int ln = 1;
    	try {
			while((textLine = cacheReader.readLine())!=null)
			 {
				textFileMap.put(new IntWritable(ln), textLine);
				ln++;
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    }
	  
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			
		    String line = value.toString();
		    StringTokenizer itr = new StringTokenizer(line.toLowerCase());
			
			if (itr.countTokens() > 2){
			    while(itr.hasMoreTokens()) {

			      docID.set(Integer.parseInt(itr.nextToken()));

			      wordID.set(Integer.parseInt(itr.nextToken()));

			      wordCount.set(Integer.parseInt(itr.nextToken()));
			      
			      if (textFileMap.size() != 0 && textFileMap.containsKey(wordID))
			      {
			        word.set(textFileMap.get(wordID));
			      }
			    }
			    output.collect(word, wordCount);
			 }
		}
}

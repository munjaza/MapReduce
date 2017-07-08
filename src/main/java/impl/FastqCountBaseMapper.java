package impl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class FastqCountBaseMapper 
  extends Mapper<Object, Text, Text, LongWritable> {
    
    private Map<Character, Long> dnaBaseCounter = null;  
    private LongWritable counter = new LongWritable();
	private Text base = new Text();
 	private static final Logger LOG = Logger.getLogger(FastqCountBaseMapper.class);

    protected void setup(Context context)
       throws IOException, InterruptedException {
       dnaBaseCounter = new HashMap<Character, Long>();  
    }

    protected void cleanup(Context context)
        throws IOException, InterruptedException {
		for (Map.Entry<Character, Long> entry : dnaBaseCounter.entrySet()) {       
           base.set(entry.getKey().toString());
           counter.set(entry.getValue());
           context.write(base, counter);      
        }
    }
    
    public void debug(String[] tokens) {
	   LOG.info("debug(): tokens.length="+tokens.length);
	   for(int i=0; i < tokens.length; i++){
	   		LOG.info("debug(): i="+i+ "\t tokens[i]="+tokens[i]);
       }
    }
    
    public void map(Object key, Text value, Context context) 
       throws IOException, InterruptedException {
	   String fastqRecord = value.toString();
	   String[] lines = fastqRecord.split(",;,");
	   //debug(lines);
	   String sequence = lines[1].toLowerCase();
	   char[] array = sequence.toCharArray(); 
	   for(char c : array){
	      Long v = dnaBaseCounter.get(c);
	      if (v == null) {
		   dnaBaseCounter.put(c, 1l);
	      }
	      else {
		   dnaBaseCounter.put(c, v+1l);
	      }
	   }
    }
}

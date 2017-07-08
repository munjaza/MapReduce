package wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCount2 extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount2.class);
    private String[] remainingArgs;

    public WordCount2(Configuration config) {
        super();
        setConf(config);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        WordCount2 mr = new WordCount2(config);
        ToolRunner.run(config, mr, args);
    }

    protected boolean validateAndParseInput(String[] args) {

        GenericOptionsParser optionParser;
        try {
            optionParser = new GenericOptionsParser(getConf(), args);
            remainingArgs = optionParser.getRemainingArgs();
            if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
                LOGGER.error("Usage: wordcount <in> <out> [-skip skipPatternFile]");
                System.exit(2);
            }
        } catch (IOException e) {
            LOGGER.error(e.getLocalizedMessage());
        }

        LOGGER.info("Input validation succeeded");
        return true;
    }

    public int run(String[] args) throws Exception {
        if (!validateAndParseInput(args)) {
            throw new RuntimeException("Losi ulazni parametri!");
        }

        Job job = Job.getInstance(getConf(), "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        if(args[1].equals("output/")) {
            FileSystem.get(getConf()).delete(new Path(args[1]), true);
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

}

package impl;

import fastq.FastqInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FastqCountBaseDriver extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(FastqCountBaseDriver.class);

	public int run(String [] args) throws Exception {

		logger.info("run(): input  args[0]="+args[0]);
		logger.info("run(): output args[1]="+args[1]);

		Job job = Job.getInstance(getConf(), "Fastq count");
		job.setJarByClass(FastqCountBaseDriver.class);
		job.setJobName("FastqCountBaseDriver");
		
		job.setInputFormatClass(FastqInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);		
		
		job.setMapperClass(FastqCountBaseMapper.class);
		job.setReducerClass(FastqCountBaseReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		if(args[1].equals("output/")) {
			FileSystem.get(getConf()).delete(new Path(args[1]), true);
			TextOutputFormat.setOutputPath(job, new Path(args[1]));
		}

		boolean status = job.waitForCompletion(true);
		logger.info("run(): status="+status);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			logger.info("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <output-dir>");
			throw new IllegalArgumentException("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <output-dir>");
		}

		String inputDir = args[0];
		logger.info("inputDir="+inputDir);

		String outputDir = args[1];
		logger.info("outputDir="+outputDir);

		int ret = ToolRunner.run(new FastqCountBaseDriver(), args);
		System.exit(ret);
	}
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO
		/*FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

		Job jobA = Job.getInstance(this.getConf(), "Link Count");
		jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

		jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

		jobA.setJarByClass(OrphanPages.class);
        jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(conf, "Orphan Pages");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(NullWritable.class);

		//todo
		jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);
	
		jobB.setMapperClass(OrphanPagesMap.class);
        jobB.setReducerClass(OrphanPagesReduce.class);
        jobB.setNumReduceTasks(1);
	
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

	    jobB.setJarByClass(OrphanPages.class);
		return jobB.waitForCompletion(true) ? 0 : 1;*/
		
		Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        
		job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
	
	public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        /*List<String> links;
		@override
		public void Setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();
            String linksPath = conf.get("links");
            this.links = Arrays.asList(readHDFSFile(links, conf).split("\n"));
		}*/
		
		@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
			String line = value.toString();
			String splitArray[] = line.split(":");
			
			context.write(new IntWritable(Integer.parseInt(splitArray[0])),new IntWritable(0));
			
			StringTokenizer tokenizer = new StringTokenizer(splitArray[1], " ");
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
				String nextToken1 = nextToken.trim().toLowerCase();
				context.write(new IntWritable(Integer.parseInt(nextToken1)), new IntWritable(1));
			}		
        }
    }
	
	/*public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
        }
    }*/

	/*public static class OrphanPageMap extends Mapper<IntWritable, IntArrayWritable, IntWritable, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
			String line = value.toString
			
			context.write(line,key);
        }
    }*/
    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum == 0)
				context.write(key,NullWritable.get());
			
        }
    }
}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Integer;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
		Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);
		
		jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);
		//jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(PopularityLeague.class);
      /*  jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Popularity League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setMapperClass(PopularityLeagueMap.class);
        jobB.setReducerClass(PopularityLeagueReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);*/
        return jobA.waitForCompletion(true) ? 0 : 1;
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
	
    // TODO
	public static class LinkCountMap extends Mapper<Object, Text, Text, IntWritable> {
        
		List<String> league;
		@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordsPath = conf.get("league");
            this.league = Arrays.asList((readHDFSFile(stopWordsPath, conf).split("\n")));
        }
		
		@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
			String line = value.toString();
			String splitArray[] = line.split(":");
			
			if(league.contains(line))
				context.write(new Text(splitArray[0]),new IntWritable(0));
			
			StringTokenizer tokenizer = new StringTokenizer(splitArray[1], " ");
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
				String nextToken1 = nextToken.trim().toLowerCase();
				
				if(league.contains(nextToken1))
					context.write(new Text(nextToken1), new IntWritable(1));
			}		
        }		
    }

    public static class LinkCountReduce extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
        private TreeSet<Pair<Integer, Integer>> countToTitleMap = new TreeSet<Pair<Integer, Integer>>();
		private ArrayList<Integer> processed = new ArrayList<Integer>();
		@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			countToTitleMap.add(new Pair<Integer, Integer>((Integer)sum, Integer.parseInt(key.toString())));
			//context.write(key,new IntWritable(sum));			
        }
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO
			int i=0,counter=0;
			boolean skip =  false;
			for (Pair<Integer, Integer> item : countToTitleMap) {
				//Integer[] strings = {item.second, item.first};
                //IntArrayWritable val = new IntArrayWritable(strings);
                //context.write(item.second, item.first);
				
				if(!processed.contains(item.first)){
					skip = false;
					context.write(new IntWritable(item.second), new IntWritable(i));
					processed.add(item.first);
				}
				else{
					skip = true;
					context.write(new IntWritable(item.second), new IntWritable(counter));
				}
				if(!skip)
					counter = i;
				i++;
            }
        }
    }
	public static class PopularityLeagueMap extends Mapper<Text, Text, IntWritable, IntWritable> {
        List<String> league;
		@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordsPath = conf.get("league");
            this.league = Arrays.asList((readHDFSFile(stopWordsPath, conf).split("\n")));
        }
        // TODO
		
		@Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
			Integer count = Integer.parseInt(value.toString());
            Integer word = Integer.parseInt(key.toString());
			int k=0,rank=16;	
			for(int i=0;i < league.size();i++){
				if(count < Integer.parseInt(league.get(i)))
					continue;
				//context.write(new IntWritable(k),new IntWritable(i));
				else
					k++;
			}
			context.write(new IntWritable(word),new IntWritable(word));
        }
    }
	
	public static class PopularityLeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

//for (Pair<Integer, Integer> item: countToTitleMap) {
			int sum=0;
			for (IntWritable val : values) {
				sum += val.get();
			}
				//IntWritable word = new IntWritable(sum);
				//IntWritable value = new IntWritable(key);
				context.write(key, new IntWritable(sum));
			//}
			
        }
    }
}	
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
// dit schrijft onderstaande cell naar een file in de huidige directory
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Jullie gaan NOOIT moeten coderen in Java!!!!
public class WordCount {

    // Mapper klasse voor de map-functie te doen - tussen de <> - input-key, intput-value, output-key, output-value
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // splits de input string in tokens (standaard op spatie splitsen)
            StringTokenizer itr = new StringTokenizer(value.toString());
            // itereer over alle woorden
            while (itr.hasMoreTokens()){
                // key is het huidige woord
                word.set(itr.nextToken());
                // emit key,value paar
                context.write(word, one);
            }
        }
    }

    // Reducer klasse voor de reduce-functie te doen - tussen de <> - input-key, intput-value, output-key, output-value
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

   // configure the MapReduce program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count java");
        job.setJarByClass(WordCount.class);
        // configure mapper
        job.setMapperClass(TokenizerMapper.class);
        // configure combiner (soort van reducer die draait op mapping node voor performantie)
        job.setCombinerClass(IntSumReducer.class);
        // configure reducer
        job.setReducerClass(IntSumReducer.class);
        // set output key-value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set input file (first argument passed to the program)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set output file  (second argument passed to the program)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // In this case, we wait for completion to get the output/logs and not stop the program to early.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

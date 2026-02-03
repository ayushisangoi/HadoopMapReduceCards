package mapredproj;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;


  


public class HW2MissingPoker {
    public static final List<String> SHAPES = Arrays.asList("HEARTS", "CLUBS", "DIAMONDS", "SPADES" );
    public static final List<String> NUMBERS = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13");

    public static class c_MAP extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      /*
        *Mapper Function
        */
        public void map(Text value, LongWritable key, OutputCollector<Text, Text> output, Reporter outpt) throws IOException {

            String[] sepline = value.toString().split(",");
            /* 
            String shape = sepline[0].toLowerCase().trim();
            String num = sepline[1].toLowerCase().trim();
            */
            String s = sepline[0].replace("\"", "");
            String shape= s.toLowerCase().trim();

            String n = sepline[1].replace("\"", "");
            String num = n.toLowerCase().trim()

            
            /* 
            Checks if deck is missing cards or not
            */

            if (HW2MissingPoker.SHAPES.contains(shape)) {
                System.out.println("Found it!");
                for (String s : HW2MissingPoker.SHAPES) {
                    if (s.equals(shape)) {
                        output.collect((Object)new Text(shape), (Object)new Text(num));
                        System.out.println("We really found it!");
                        continue;
                    }
                    output.collect((Object)new Text(shape), (Object)new Text(""));
                    System.out.println("jk no we didn't");
                }
            }
        }
    }


    public static class c_RED extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
       /*
        *Reducer Function to count missing cards
        */
        public void reduce(Iterator<Text> valuesCaterpillar,  Text noshape, OutputCollector<Text, Text> wasntfound, Reporter outpt) throws IOException {
          
           /*
             Iterates through mappers results to compile the "wasntfounds" 
            */
            ArrayList<String> listofvalues = new ArrayList<String>();
            while (valuesCaterpillar.hasNext()) {

                listofvalues.add(valuesCaterpillar.next().toString());
                
            }
            for (String nonumber : MissingCardFinder.NUMBERS) {
              
                if (listofvalues.contains(nonumber)) continue;
                wasntfound.collect((Object)noshape, (Object)new Text(nonumber));
                
            }
        }
    }
    public static void main(String[] args) throws IOException {
       /*
        *Main Function
        does all initiations and starts the mapreduce process
        */
        JobConf conf = new JobConf((Class)HW2MissingPoker.class);
        conf.setJobName("FindingMissingCards");

        conf.setMapperClass((Class)c_MAP.class);
        conf.setReducerClass((Class)c_RED.class);

        conf.setInputFormat((Class)TextInputFormat.class);
        conf.setOutputFormat((Class)TextOutputFormat.class);

        conf.setOutputKeyClass((Class)Text.class);
        conf.setOutputValueClass((Class)Text.class);

        FileInputFormat.setInputPaths((JobConf)conf, (Path[])new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath((JobConf)conf, (Path)new Path(args[1]));
        
        System.out.println("Starting the process!");
        
        JobClient.runJob((JobConf)conf);
    }

}











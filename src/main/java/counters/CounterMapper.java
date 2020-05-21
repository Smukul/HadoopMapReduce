package counters;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

enum LOCATION {
    TOTAL, BANGALORE,CHENNAI, HYDERABAD
}
public class CounterMapper extends Mapper<LongWritable, Text,Text, Text> {

    private Text storeLocation = new Text();
    private Text data = new Text();
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        //file record - //  1194208577,sofa,39,3,Hyderabad
        /* total records processed by all mappers */
        context.getCounter(LOCATION.TOTAL).increment(1);
        String[] values = line.toString().split(",");
        /* location counters */
        if(values[4].equalsIgnoreCase("bangalore")){
            context.getCounter(LOCATION.BANGALORE).increment(1);
        } else if(values[4].equalsIgnoreCase("chennai")){
            context.getCounter(LOCATION.CHENNAI).increment(1);
        } else if(values[4].equalsIgnoreCase("hyderabad")){
            context.getCounter(LOCATION.HYDERABAD).increment(1);
        } else {
            throw new RuntimeException("No Such City Found.");
        }
        /* sale counters  - dynamic */
        if(Integer.parseInt(values[3]) < 10){
            context.getCounter("SALES","LOW_SALES").increment(1);
        }
        if(Integer.parseInt(values[2]) > 500){
            context.getCounter("SALES","HIGH_REVENUE").increment(1);
        }
        storeLocation.set(values[4]);
        data.set(values[2]+","+values[3]);
        context.write(storeLocation,data);
    }
}

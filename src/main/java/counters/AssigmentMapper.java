package counters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

enum RECORDS {
    BADRECORDS, GOODRECORDS, NULLRECORDS
}

public class AssigmentMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] words = line.toString().split(",");
        for(String word : words){
            try{
                Integer.parseInt(word);
                context.getCounter(RECORDS.BADRECORDS).increment(1);
                context.write(new Text(word.trim()),new IntWritable(1));
            } catch (NumberFormatException ex){
                context.getCounter(RECORDS.GOODRECORDS).increment(1);
                context.write(new Text(word.trim()),new IntWritable(1));
            } catch (NullPointerException ex){
                context.getCounter(RECORDS.NULLRECORDS).increment(1);
            }
        }
    }
}

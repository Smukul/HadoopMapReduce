package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmpMapper extends Mapper<LongWritable, Text,Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] empData = value.toString().split(" ");
        context.write(new Text(empData[0]),new Text("Emp,"+empData[1]));
    }
}

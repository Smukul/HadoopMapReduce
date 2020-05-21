package bankloyalcustomer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccountMapper extends Mapper<LongWritable, Text,Text,Text> {

    public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
        //input = OMOI808692OZ,1245015582,savings,3667,822,no
        String[] values = input.toString().split(",");
        context.write(new Text(values[0]),new Text("A,"+values[1]+","+values[3]+","+values[4]+","+values[5]));
    }
}

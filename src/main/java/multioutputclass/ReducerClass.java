package multioutputclass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, Text,Text,Text> {
    //Reducer Input
    //   DJPX255251        [{ Arthur,HR,6397} { Arthur,HR,6597} {Arthur,HR,6797} ]
    private MultipleOutputs<Text,Text> outputs;

    @Override
    protected void setup(Context context){
        outputs = new MultipleOutputs<Text, Text>(context);
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalSalary = 0;
        String dept ="";
        String name = "";
        for(Text value : values){
            String[] arr = value.toString().split(",");
            name = arr[0];
            dept = arr[1];
            totalSalary += Integer.parseInt(arr[2]);
        }
        /* output employee salaray to department file */
        if (dept.equalsIgnoreCase("HR"))
        {
            outputs.write("HR", key, new Text(name + "," + totalSalary));
        }
        else if (dept.equalsIgnoreCase("Accounts"))
        {
            outputs.write("Accounts", key, new Text(name + "," + totalSalary));
        }
    }

    @Override
    protected void cleanup(Context c) throws IOException, InterruptedException {
        outputs.close();
    }
}

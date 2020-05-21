package join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerClass extends Reducer<Text, Text,Text,Text> {

    public void reduce(Text empId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> empList = new ArrayList<String>();
        List<String> addList = new ArrayList<String>();
        for(Text value : values){
            String[] data = value.toString().split(",");
            if(data[0].equalsIgnoreCase("Emp")){
                empList.add(data[1]);
            } else{
                addList.add(data[1]);
            }
        }
        if(!empList.isEmpty() && !addList.isEmpty()){
            for(String empData : empList){
                for(String addData : addList){
                    context.write(empId,new Text(empData+" , "+addData));
                }
            }
        }
    }
}

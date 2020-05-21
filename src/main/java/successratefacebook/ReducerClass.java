package successratefacebook;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerClass extends Reducer<Text, Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,String> cityData = new HashMap<String, String>();
        for(Text value : values){
            String[] data = value.toString().split(",");
            String city = data[0].trim();
            int clickCount = Integer.parseInt(data[1].trim());
            int conversionRate = Integer.parseInt(data[2].trim());
            Double successRate = new Double(conversionRate/(clickCount*1.0)*100);
            if(cityData.containsKey(city)){
                String s1 = cityData.get(city);
                String[] hValues = s1.split(",");
                Double totalSuccRate = Double.parseDouble(hValues[0]) + successRate;
                int totalCount = Integer.parseInt(hValues[1]) + 1;
                cityData.put(city, totalSuccRate + "," + totalCount);
            } else{
                cityData.put(city, successRate+",1");
            }
        }
        System.out.println(cityData.toString());
        for(Map.Entry<String,String> entry: cityData.entrySet()){
            String[] V1 = entry.getValue().split(",");
            Double avgSccRate = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);
            context.write(key, new Text(entry.getKey() + "," + avgSccRate));
        }
    }
}

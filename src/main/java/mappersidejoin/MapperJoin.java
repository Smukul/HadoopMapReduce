package mappersidejoin;

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

public class MapperJoin extends Mapper<LongWritable, Text,Text, IntWritable> {
    //key: product_id  value: product_price
    private Map<String,String> prodData = new HashMap<String, String>();
    // key: store_id  value: store_location
    private Map<String,String> storeData = new HashMap<String, String>();

    protected void setup(Context context) throws IOException {
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        BufferedReader br;
        String line = "";
        for(Path cacheFile : cacheFilesLocal){
            if(cacheFile.getName().toString().trim().equalsIgnoreCase("store.txt")){
                br = new BufferedReader(new FileReader(cacheFile.toString()));
                line = br.readLine();
                while(line != null){
                    String[] store = line.split(",");
                    storeData.put(store[0],store[1]);
                    line = br.readLine();
                }
            } else if(cacheFile.getName().toString().trim().equalsIgnoreCase("product.txt")){
                br = new BufferedReader(new FileReader(cacheFile.toString()));
                line = br.readLine();
                while(line != null){
                    String[] store = line.split(",");
                    prodData.put(store[0],store[3]);
                    line = br.readLine();
                }
            }
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] saleInfo = value.toString().split(",");
        String storeId = saleInfo[0];
        int productSale = Integer.parseInt(saleInfo[3].trim());
        int productPrice = Integer.parseInt(prodData.get(saleInfo[1]));
        int revanue = productSale*productPrice;
        String storeLocation = storeData.get(storeId);
        context.write(new Text(storeId+" "+storeLocation),new IntWritable(revanue));
    }
}

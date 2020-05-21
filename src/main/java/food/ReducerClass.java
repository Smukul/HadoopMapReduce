package food;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerClass extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> input, Context context) throws IOException, InterruptedException {
        //input // JXJY167254JK [ { 18-06-2017,2,Late delivery}  { 21-06-2017,2,Late delivery} { 20-06-2017,3,Stale food} { 17-07-2017,2,Late delivery }... ]
        Map<Integer,Integer> orderPerMonth =  new HashMap<Integer, Integer>(); //key= month ; value = total orders in that month
        Map<String,Integer> feedbacks =  new HashMap<String, Integer>(); // feedback , number of feedbacks
        for(Text data : input){
            String[] values = data.toString().split(","); // values [ {21-06-2017} {2} {Late delivery} ]

            int month = Integer.parseInt(values[0].split("-")[1].trim());             // month = 06
            int ratings = Integer.parseInt(values[1].trim());                         // ratings = 2
            String feedback = values[2].trim();                                       //  feedback  = Late delivery

            /* count number of orders per month */
            if (orderPerMonth.containsKey(month)) {
                orderPerMonth.put(month, orderPerMonth.get(month) + 1);
            }  else   {
                orderPerMonth.put(month, 1);
            }
            if (ratings <= 3) {
                /* count feedback types */
                if (feedbacks.containsKey(feedback))  {
                    feedbacks.put(feedback, feedbacks.get(feedback) + 1);
                }  else  {
                    feedbacks.put(feedback, 1);
                }
            }
        }

        System.out.println("*****************************");
        System.out.println(key.toString());
        int prevMonthOrders = 0;
        int declineCount = 0;
        double orderRate = 0;
        for (int i=6; i<=9; ++i){
            // prevMonthOrders = 7
            Integer currMonthOrders = orderPerMonth.get(i);
            // currMonthOrders = 3
            if (currMonthOrders == null) {
                currMonthOrders = 0;
            }
            if (prevMonthOrders > 0) {
                orderRate = ((1.0 * currMonthOrders) / prevMonthOrders) * 100;           // orderRate = 0
            }
            else {
                orderRate = 0;
            }
            // churn condition
            if ((currMonthOrders < prevMonthOrders) &&(orderRate <50.0)) {
                declineCount++;                                                     // declineCount = 3
            } else  {
                declineCount = 0;
            }

            System.out.println(i + ", " + declineCount + ", " + orderRate + ", " + currMonthOrders + ", " + prevMonthOrders);

            if (declineCount == 3)  {
                /* booking is declining for 3 consecutive months */
                String outFeedback = "";
                int feedbackCount = 0;
                for (Map.Entry<String, Integer> e : feedbacks.entrySet()) {
                    if (e.getValue() > feedbackCount)  {
                        outFeedback = e.getKey();
                        feedbackCount = e.getValue();
                    }
                }
                context.write(key, new Text(outFeedback));
                //    JXJY167254JK    Late delivery
            }
            prevMonthOrders = currMonthOrders;
        }
    }

}

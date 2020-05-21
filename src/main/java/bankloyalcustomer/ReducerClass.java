package bankloyalcustomer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> input, Context context) throws IOException, InterruptedException {
        //input //OMOI808692OZ  [{P,Allison,Abbott} {A,1245015582,3667,822,no} {A,1245015582,5806,1035,no} {A,1245015582,1601,635,no} {A,1245015582,4189,802,no} ]
        String fname="",lname="",accNumber="";
        int totalDeposit=0;
        for(Text in : input){
            String[] values = in.toString().split(",");
            if(values[0].equals("P")){
                /* customer personal details from PersonMapper */
                fname = values[1];
                lname = values[2];
            } else if(values[0].equals("A")){
                /* customer account details from AccountMapper */
                accNumber = values[1];
                /* check for red flag */
                if(values[4].equalsIgnoreCase("yes")){
                    /*
                     * ignore any other processing since customer cannot be
                     * loyal if red flag is true for any quarter
                     * No need to output anything for this customer
                     */
                    break;
                }
                /* check for quarter deposit and withdraw */
                int withdrawAmount = Integer.parseInt(values[3]);
                int depositAmount = Integer.parseInt(values[2]);
                if (withdrawAmount >= (depositAmount/2))   {
                    /* No need to process further */
                    break;
                }
                totalDeposit += depositAmount;                    //totalDeposit  = 3667
            }
            }
        /* if reached here, all loyal customer conditions met except total deposit amount */
        if (totalDeposit >= 10000)
        {
            /* id, fName, lName, accountNumber */
            context.write(key, new Text(fname + "," + lname + "," + accNumber));
        }

    }
}

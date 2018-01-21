package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

public class CalcMapper extends Mapper<Text, Text, Text, Text> {
    // public static enum counterTypes { 
    //     DANGLINGSUM
    // }

    private static Double DanglingSum;
    Long GlobalNum, DanglingNum;
    int n_reducer;
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        GlobalNum = conf.getLong("GlobalNum", 0);
        DanglingNum = conf.getLong("DanglingNum", 0);
        n_reducer = conf.getInt("n_reducer", 0);
        DanglingSum = 0.0;
    }

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration(); 
        
        String thisPageTitle = key.toString();
        String thisPageValue = value.toString();
        String thisPageRank = null;
        String thisPageLink = null;

        // input 
        // <thisPageTitle>    3.254466755622091E-5|Consolation of Philosophy

        // NOT Dangling Page
        if (thisPageValue.contains("|") ) { 
            // Split content like <3.123|linkA|linkB|linkC>
            String[] content = thisPageValue.split("\\|", 2);
            thisPageRank = content[0];       // 3.123
            thisPageLink = content[1];       // linkA|linkB|linkC

            System.out.println("thisPageLink : " + thisPageLink);

            // more than 1 links
            if (thisPageLink.contains("|")) {
                
                String[] links = thisPageLink.split("\\|");
                int totalLinks = links.length;
                System.out.println("totalLinks : " + totalLinks);
                
                // Write out to calculate PR
                for (String link : links) {
                    System.out.println("this link : " + link);
                    StringBuilder thisPageWriteOut = new StringBuilder();
    
                    // <---KEY--->   <-----------------VALUE----------------> 
                    //   <linkA>      <thisPage> | <pageRank> | <numOfLinks>
                    //   <linkB>      <thisPage> | <pageRank> | <numOfLinks>
                    thisPageWriteOut.append( thisPageTitle + "|" + thisPageRank  + "|" + Integer.toString(totalLinks) );
                    
                    // Write out to calculate PR
                    context.write(new Text(link), new Text(thisPageWriteOut.toString() ));
                }
            }
            else {
                // only 1 link
                String link = thisPageLink;
                int totalLinks = 1;
                StringBuilder thisPageWriteOut = new StringBuilder();
                thisPageWriteOut.append( thisPageTitle + "|" + thisPageRank  + "|" + Integer.toString(totalLinks) );
                
                // Write out to calculate PR
                context.write(new Text(link), new Text(thisPageWriteOut.toString() ));
            } 
        }
        else { // Dangling Page
            thisPageRank = thisPageValue;
            DanglingSum += Double.parseDouble(thisPageRank);
            // System.out.println("dangling, PR: " + thisPageRank + " ,DanglingSum: " + DanglingSum );
        }

        // Write original input to context for calculating error
        context.write(new Text(thisPageTitle), new Text("###" + value.toString()) );
    }

	protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Long ScaledDanglingSum = (long) (DanglingSum * 1E15);

        // conf.setLong("ScaledDanglingSum", ScaledDanglingSum);
        System.out.println("[CalcMapper Cleanup] DanglingSum: " + DanglingSum );
        System.out.println("[CalcMapper Cleanup] ScaledDanglingSum: " + ScaledDanglingSum);
		context.getCounter(PageRanking.counterTypes.DANGLINGSUM).setValue(ScaledDanglingSum);
	}
}

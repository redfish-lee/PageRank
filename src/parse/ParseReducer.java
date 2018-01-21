package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Arrays;
import java.util.HashSet;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {
	
	private static HashSet<String> List;
	private static long count = 0;

	public static enum counterNums {
		Dangling
    }

	public ParseReducer(){
		this.List = new HashSet<String>();
	}

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		for (Text val: value) {
			if(key.toString().charAt(0) == ' ') {
				List.add(val.toString());
			}
			else {
				if( List.contains(val.toString()) ) {
					builder.append( "|" + val.toString() );
				}
			}
		}
		if(key.toString().charAt(0) != ' ') {
			Text write_out = new Text( String.valueOf( ((double)1)/(List.size())) + builder.toString() );
			
			context.write(key, write_out);
			// dangling 
			if(!builder.toString().contains("|")) count++;
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("count: " + count);
		context.getCounter(counterNums.Dangling).setValue(count);
	}
}

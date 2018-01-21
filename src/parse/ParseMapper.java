package PageRank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
		int n_reducer = conf.getInt("n_reducer", 0);
		String input = value.toString();
		/*  Match title pattern */  
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(input);
		
		titleMatcher.find(); 
		String title = unescapeXML( titleMatcher.group(1));            
		for (int i = 0; i < n_reducer; i++) {
			Text k = new Text(" " + Integer.toString(i));
			Text v = new Text(title);
			context.write(k, v);
		}
        
		// No need capitalizeFirstLetter
		
		/*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(input);

		boolean havelink = false;
		while(linkMatcher.find()) {
			havelink = true;
			String link = capitalizeFirstLetter(unescapeXML(linkMatcher.group(1)));
			context.write(new Text(title), new Text(link));
		}
		if (havelink == false){
			context.write(new Text(title), new Text("Pure Dangling"));
		}
		// Need capitalizeFirstLetter
	}
	
	private String unescapeXML(String input) {

		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");

    }

    private String capitalizeFirstLetter(String input){

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}

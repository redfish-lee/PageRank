package PageRank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.NullWritable;

// public class SortPartitioner extends Partitioner<SortPair, NullWritable> {
	
	// private double maxValue = 11.05;
	// private double minValue = 9.0;

	// @Override
	// public int getPartition(SortPair key, NullWritable value, int numReduceTasks) {
		
	// 	int num = 0;		
	// 	double chunk = (maxValue - minValue) / numReduceTasks;
	// 	double avg = key.getAverage();
    //     num = numReduceTasks - (int)((avg - minValue) / chunk) -1;
	// 	return num;
	// }
// }

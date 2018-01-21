package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRanking extends Configured implements Tool {
    private static final double CONVERGENCE = 0.001;
    private static final int ITER_LIMIT = 30;
    private static NumberFormat nf = new DecimalFormat("00");
    
    long GlobalNum;
    long DanglingNum;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
/*
        name=input-100M
        INPUT FILE:  /user/ta/PageRank/Input/$name
        OUTPUT FILE: /user/p106062641/PageRank/input-100M
        RESULT FILE: /user/p106062641/PageRank/input-100M/result   */

        String lastResultPath = null;
        String inPath    = args[0];
        String parsePath = args[1] + "/iter00";
        String outPath   = args[1] + "/iter";
        String resPath   = args[1] + "/final";

        // Job 1: Parse XML
        boolean isCompleted = Parse(inPath, parsePath);
        if (!isCompleted) 
            System.out.println("Job 1 crashed\n\n");        
        System.out.println("Job 1: Parse XML finish!");
        System.out.println("#GlobalNum: " + GlobalNum + " #Dangling: " + DanglingNum);  
        lastResultPath = parsePath;


        // Job 2: Calculate PageRank
        double iterError = 0.0;
        // iter set default run to converge
        int iter = (args.length == 3) ? Integer.parseInt(args[2]) : ITER_LIMIT;
        System.out.println("args.length: " + args.length);
        System.out.println("total iterations: " + iter);

        for (int runs = 0; runs < iter; runs++) {
            inPath = outPath + nf.format(runs);
            lastResultPath = outPath + nf.format(runs + 1);
            System.out.println("Job 2 RankCalc, iter: " + (runs + 1));
            System.out.println("inPath  : " + inPath);
            System.out.println("outPath : " + lastResultPath);

            iterError = RankCalc(inPath, lastResultPath);
            System.out.println("iter #" + (runs + 1) + " with error: "  + iterError );
            if (iterError < CONVERGENCE) {
                System.out.println("iter #" + (runs + 1) + " achieve convergence!");
                break;
            }
        }
        System.out.println("Job 2: CalculateRank finish!");

        // Job 3: Sort
        System.out.println("[JOB 3] sort input : " + lastResultPath);
        System.out.println("[JOB 3] sort output: " + resPath);
        isCompleted = Sorting(lastResultPath, resPath);
        System.out.println("\nJob 3: SortRanking finish, return: " + isCompleted);
        
        return 0;
    }

    public boolean Parse(String inputPath, String outputPath) throws Exception {
        int num_reducer = 32;
        Double error = 0.0;
        Configuration conf = new Configuration();
        conf.setInt("n_reducer", num_reducer);

        Job job = Job.getInstance(conf, "Parse");
        job.setJarByClass(PageRanking.class);
        
        // default
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ParseMapper.class);
        job.setPartitionerClass(ParsePartitioner.class);
        job.setReducerClass(ParseReducer.class);

        // set the number of reducer
        job.setNumReduceTasks(num_reducer);
        
        // set the output class of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // add input/output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) throw new Exception("Job failed");

        // Get parse result
        GlobalNum = job.getCounters().findCounter(
            TaskCounter.MAP_INPUT_RECORDS).getValue();
        
        DanglingNum = job.getCounters().findCounter(
            ParseReducer.counterNums.Dangling).getValue();

        return true;
    }

    public static enum counterTypes { 
        ERROR,
        DANGLINGSUM
    }

    public double RankCalc(String inputPath, String outputPath) throws Exception {
        int num_reducer = 32; 
        Configuration conf = new Configuration();
        conf.setInt("n_reducer", num_reducer);
        conf.setLong("GlobalNum", GlobalNum);
        conf.setLong("DanglingNum", DanglingNum);

        Job job2 = Job.getInstance(conf, "RankCalc");
        job2.setJarByClass(PageRanking.class);
        
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(CalcMapper.class);
        job2.setReducerClass(CalcReduce.class);
        job2.setNumReduceTasks(num_reducer);
        
        // set the output class of Mapper
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        if (!job2.waitForCompletion(true)) throw new Exception("Job failed");

        // Get counter with long type
        long counterError = job2.getCounters().findCounter(PageRanking.counterTypes.ERROR).getValue();
        double sumError = ( (double) counterError / CalcReduce.SCALING );

	    return sumError;
    }

    public boolean Sorting(String inputPath, String outputPath) throws Exception {
        int num_reducer = 1;

        Configuration conf = new Configuration();
        conf.setInt("n_reducer", num_reducer);
        Job job3 = Job.getInstance(conf, "Sorting");
        job3.setJarByClass(PageRanking.class);
       
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(SortMapper.class);
        // job3.setPartitionerClass(SortPartitioner.class);
        job3.setReducerClass(SortReducer.class);
        job3.setNumReduceTasks(num_reducer);

		// set the output class of Mapper and Reducer
		job3.setMapOutputKeyClass(SortPair.class);
		job3.setMapOutputValueClass(NullWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        // job3.setOutputFormatClass(TextOutputFormat.class);

        // add input/output path
        FileInputFormat.setInputPaths(job3, new Path(inputPath));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));

        return job3.waitForCompletion(true);
    }

}

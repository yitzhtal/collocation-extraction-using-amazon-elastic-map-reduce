package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapreduces.FourthMapReduce;
import mapreduces.ThirdMapReduce;

public class CollocationExtraction {

    private static final String FIRST_INTERMEDIATE_OUTPUT = "s3n://collocation-extraction-assignment/pipeline/first_intermediate_output";
    private static final String SECOND_INTERMEDIATE_OUTPUT = "s3n://collocation-extraction-assignment/pipeline/second_intermediate_output";
    private static final String THIRD_INTERMEDIATE_OUTPUT = "s3n://collocation-extraction-assignment/pipeline/third_intermediate_output";
    private static final String FOURTH_INTERMEDIATE_OUTPUT = "s3n://collocation-extraction-assignment/pipeline/fourth_intermediate_output";
    public static final String OUTPUT = "s3n://collocation-extraction-assignment/output/output.txt";


    @SuppressWarnings("unchecked")
	public static boolean setAndRunMapReduceJob (String jobName,Configuration conf, Class<CollocationExtraction> MapReduceClass,Class Mapper, Class Reducer,
                                          Class MapOutputKey,Class MapOutputValue,Class ReduceOutputKey, Class ReduceOutputValue,
                                          String Input, String Output, boolean isLZO, Class partitionerClass) throws Exception{
        Job myJob = new Job(conf, jobName);
        myJob.setJarByClass(MapReduceClass);
        if(Mapper != null) myJob.setMapperClass(Mapper);
        if(Reducer != null) myJob.setReducerClass(Reducer);

        //Mapper`s output
        myJob.setMapOutputKeyClass(MapOutputKey);
        myJob.setMapOutputValueClass(MapOutputValue);

        if(isLZO) myJob.setInputFormatClass(SequenceFileInputFormat.class);

        //Reducer`s output
        if(ReduceOutputKey != null) myJob.setOutputKeyClass(ReduceOutputKey);
        if(ReduceOutputValue != null) myJob.setOutputValueClass(ReduceOutputValue);
        if(partitionerClass != null) myJob.setPartitionerClass(partitionerClass);

        TextInputFormat.addInputPath(myJob, new Path(Input));
        TextOutputFormat.setOutputPath(myJob, new Path(Output));

        return myJob.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 3 || args.length > 5) {
            System.out.println("The number of arguments is suppose to be 5!");
            return;
        }

        String minPmi = args[0];
        String relMinPmi = args[1];
        String language = args[2];	
        String isStopWordsIncluded = args[3];
        String INPUT = args[4];

        System.out.println("CollocationExtraction :: has started...\n");
        System.out.println("CollocationExtraction :: Arguments received :: minPmi = " + minPmi);
        System.out.println("CollocationExtraction :: Arguments received :: relMinPmi = " + relMinPmi);
        System.out.println("CollocationExtraction :: Arguments received :: language = " + language);
        System.out.println("CollocationExtraction :: Arguments received :: isStopWordsIncluded = " + isStopWordsIncluded + "\n\n");
        System.out.println("CollocationExtraction :: Arguments received :: INPUT = " + INPUT + "\n\n");

        Configuration conf = new Configuration();
        conf.set("language",language);
        conf.set("minPmi",minPmi);
        conf.set("relMinPmi",relMinPmi);
        conf.set("isStopWordsIncluded",isStopWordsIncluded);
        conf.set("mapreduce.map.java.opts","-Xmx512m");
        conf.set("mapreduce.reduce.java.opts","-Xmx1536m");
        conf.set("mapreduce.map.memory.mb","768");
        conf.set("mapreduce.reduce.memory.mb","2048");
        conf.set("yarn.app.mapreduce.am.resource.mb","2048");
        conf.set("yarn.scheduler.minimum-allocation-mb","256");
        conf.set("yarn.scheduler.maximum-allocation-mb","12288");
        conf.set("yarn.nodemanager.resource.memory-mb","12288");
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.5");

        boolean waitForJobComletion = setAndRunMapReduceJob("FirstMapReduce",conf, main.CollocationExtraction.class,
                mapreduces.FirstMapReduce.FirstMapReduceMapper.class, mapreduces.FirstMapReduce.FirstMapReduceReducer.class,
                corpus.Bigram.class,LongWritable.class,
                corpus.Bigram.class,LongWritable.class,
                INPUT,FIRST_INTERMEDIATE_OUTPUT,true,null);

        if (waitForJobComletion) {
            waitForJobComletion = setAndRunMapReduceJob("SecondMapReduce", conf, main.CollocationExtraction.class,
                    mapreduces.SecondMapReduce.SecondMapReduceMapper.class, mapreduces.SecondMapReduce.SecondMapReduceReducer.class,
                    corpus.Bigram.class, LongWritable.class,
                    corpus.Bigram.class, IntWritable.class,
                    FIRST_INTERMEDIATE_OUTPUT, SECOND_INTERMEDIATE_OUTPUT,false,mapreduces.SecondMapReduce.SecondMapReducePartitioner.class);
            if (waitForJobComletion) {
                waitForJobComletion = setAndRunMapReduceJob("ThirdMapReduce", conf, main.CollocationExtraction.class,
                        ThirdMapReduce.ThirdMapReduceMapper.class, ThirdMapReduce.ThirdMapReduceReducer.class,
                        corpus.Bigram.class, Text.class,
                        corpus.Bigram.class, Text.class,
                        SECOND_INTERMEDIATE_OUTPUT, THIRD_INTERMEDIATE_OUTPUT,false,mapreduces.ThirdMapReduce.ThirdMapReducePartitioner.class);

                if (waitForJobComletion) {
                    waitForJobComletion = setAndRunMapReduceJob("FourthMapReduce", conf, main.CollocationExtraction.class,
                            FourthMapReduce.FourthMapReduceMapper.class, FourthMapReduce.FourthMapReduceReducer.class,
                            corpus.Bigram.class, Text.class,
                            corpus.Bigram.class, Text.class,
                            THIRD_INTERMEDIATE_OUTPUT, FOURTH_INTERMEDIATE_OUTPUT,false,mapreduces.FourthMapReduce.FourthMapReducePartitioner.class);
                    if (waitForJobComletion) {
                        waitForJobComletion = setAndRunMapReduceJob("FifthMapReduce", conf, main.CollocationExtraction.class,
                                mapreduces.FifthMapReduce.FifthMapReduceMapper.class, mapreduces.FifthMapReduce.FifthMapReduceReducer.class,
                                corpus.CalculatedBigram.class, Text.class,
                                corpus.CalculatedBigram.class, Text.class,
                                FOURTH_INTERMEDIATE_OUTPUT, OUTPUT,false,mapreduces.FifthMapReduce.FifthMapReducePartitioner.class);
                        if (waitForJobComletion) {
                            System.out.println("CollocationExtraction :: Done running all map reduces successfully!");
                            return;
                        }
                    }
                }
            }
        }
        System.out.println("CollocationExtraction :: an error has occurred during one of the jobs.");
        return;
    }
}




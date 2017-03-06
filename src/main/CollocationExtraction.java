package main;

import mapreduces.FourthMapReduce;
import mapreduces.ThirdMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;


public class CollocationExtraction {

    public static final String INPUT = "C:\\IdeaProjects\\CollocationExtractionUsingAmazonElasticMapReduceProject\\input.txt";
    private static final String FIRST_INTERMEDIATE_OUTPUT = "pipeline\\first_intermediate_output";
    private static final String SECOND_INTERMEDIATE_OUTPUT = "pipeline\\second_intermediate_output";
    private static final String THIRD_INTERMEDIATE_OUTPUT = "pipeline\\third_intermediate_output";
    private static final String FOURTH_INTERMEDIATE_OUTPUT = "pipeline\\fourth_intermediate_output";
    public static final String OUTPUT = "C:\\IdeaProjects\\CollocationExtractionUsingAmazonElasticMapReduceProject\\pipeline\\OUTPUT";

    public static boolean setAndRunMapReduceJob (String jobName,Configuration conf, Class MapReduceClass,Class Mapper, Class Reducer,
                                          Class MapOutputKey,Class MapOutputValue,Class ReduceOutputKey, Class ReduceOutputValue,
                                          String Input, String Output) throws Exception{
        Job myJob = new Job(conf, jobName);
        myJob.setJarByClass(MapReduceClass);
        if(Mapper != null) myJob.setMapperClass(Mapper);
        if(Reducer != null) myJob.setReducerClass(Reducer);

        //Mapper`s output
        myJob.setMapOutputKeyClass(MapOutputKey);
        myJob.setMapOutputValueClass(MapOutputValue);

        //Reducer`s output
        if(ReduceOutputKey != null) myJob.setOutputKeyClass(ReduceOutputKey);
        if(ReduceOutputValue != null) myJob.setOutputValueClass(ReduceOutputValue);

        TextInputFormat.addInputPath(myJob, new Path(Input));
        TextOutputFormat.setOutputPath(myJob, new Path(Output));

        return myJob.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 3 || args.length > 4) {
            System.out.println("The number of arguments is suppose to be 4!");
            return;
        }

        String minPmi = args[0];
        String relMinPmi = args[1];
        String language = args[2];
        String isStopWordsIncluded = args[3];

        System.out.println("CollocationExtraction :: has started...\n");
        System.out.println("CollocationExtraction :: Arguments received :: minPmi = " + minPmi);
        System.out.println("CollocationExtraction :: Arguments received :: relMinPmi = " + relMinPmi);
        System.out.println("CollocationExtraction :: Arguments received :: language = " + language);
        System.out.println("CollocationExtraction :: Arguments received :: isStopWordsIncluded = " + isStopWordsIncluded + "\n\n");

        Configuration conf = new Configuration();
        conf.set("language",language);
        conf.set("minPmi",minPmi);
        conf.set("relMinPmi",relMinPmi);
        conf.set("isStopWordsIncluded",isStopWordsIncluded);

        FileSystem fs = FileSystem.get(conf);

        boolean waitForJobComletion = setAndRunMapReduceJob("FirstMapReduce",conf, mapreduces.FirstMapReduce.class,
                mapreduces.FirstMapReduce.FirstMapReduceMapper.class, mapreduces.FirstMapReduce.FirstMapReduceReducer.class,
                corpus.Bigram.class,IntWritable.class,
                corpus.Bigram.class,IntWritable.class,
                INPUT,FIRST_INTERMEDIATE_OUTPUT);

        if (waitForJobComletion) {
            waitForJobComletion = setAndRunMapReduceJob("SecondMapReduce", conf, mapreduces.SecondMapReduce.class,
                    mapreduces.SecondMapReduce.SecondMapReduceMapper.class, mapreduces.SecondMapReduce.SecondMapReduceReducer.class,
                    corpus.Bigram.class, IntWritable.class,
                    corpus.Bigram.class, IntWritable.class,
                    FIRST_INTERMEDIATE_OUTPUT, SECOND_INTERMEDIATE_OUTPUT);
            if (waitForJobComletion) {
                waitForJobComletion = setAndRunMapReduceJob("ThirdMapReduce", conf, ThirdMapReduce.class,
                        ThirdMapReduce.ThirdMapReduceMapper.class, ThirdMapReduce.ThirdMapReduceReducer.class,
                        corpus.Bigram.class, Text.class,
                        corpus.Bigram.class, Text.class,
                        SECOND_INTERMEDIATE_OUTPUT, THIRD_INTERMEDIATE_OUTPUT);

                if (waitForJobComletion) {
                    waitForJobComletion = setAndRunMapReduceJob("FourthMapReduce", conf, FourthMapReduce.class,
                            FourthMapReduce.FourthMapReduceMapper.class, FourthMapReduce.FourthMapReduceReducer.class,
                            corpus.Bigram.class, Text.class,
                            corpus.Bigram.class, Text.class,
                            THIRD_INTERMEDIATE_OUTPUT, FOURTH_INTERMEDIATE_OUTPUT);
                    if (waitForJobComletion) {
                        waitForJobComletion = setAndRunMapReduceJob("FifthMapReduce", conf, mapreduces.FifthMapReduce.class,
                                mapreduces.FifthMapReduce.FifthMapReduceMapper.class, mapreduces.FifthMapReduce.FifthMapReduceReducer.class,
                                corpus.CalculatedBigram.class, Text.class,
                                corpus.CalculatedBigram.class, Text.class,
                                FOURTH_INTERMEDIATE_OUTPUT, OUTPUT);
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




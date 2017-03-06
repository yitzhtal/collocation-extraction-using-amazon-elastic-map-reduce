package main;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

/**
 * Created by Tal on 05/03/2017.
 */
public class ElasticMapReduceRunner {
    public static void main(String[] args) {

        System.out.println("ElasticMapReduceRunner :: has started running...");
        AWSCredentials credentials = null;
        try {
            credentials = new PropertiesCredentials(CollocationExtraction.class.getResourceAsStream("/main/resources/AWSCredentials.properties"));
            String accessKey = credentials.getAWSAccessKeyId();
            String secretKey = credentials.getAWSSecretKey();
            if (accessKey == null || secretKey == null) {
                System.out.println("ElasticMapReduceRunner :: accessKey / secret key are null.");
            } else {
                System.out.println("ElasticMapReduceRunner :: accessKey = " + accessKey);
                System.out.println("ElasticMapReduceRunner :: secretKey = " + secretKey);
            }
        } catch(Exception e) {
            System.out.println("ElasticMapReduceRunner :: cant read from Properties file :: " + e.toString());
            return;
        }

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://collocation-extraction-project/CollocationsExtractionUsingAmazonElasticMapReduce.jar") // This should be a full map reduce application.
                .withMainClass("main.CollocationExtraction")
                .withArgs("s3n://collocation-extraction-project/input/", "s3n://collocation-extraction-project/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType("m1.large")
                .withSlaveInstanceType("m1.large")
                .withHadoopVersion("2.2.0").withEc2KeyName("hardwell") //this is the same as in assignment 1 :)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://collocation-extraction-project/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

package main;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class ElasticMapReduceRunner {

	public static String propertiesFilePath = "C:\\IdeaProjects\\CollocationExtractionUsingAmazonElasticMapReduceProject\\src\\main\\resources\\AWSCredentials.properties";

    public static void main(String[] args) throws FileNotFoundException, IOException {
        System.out.println("ElasticMapReduceRunner :: has just started..");
        System.out.println("ElasticMapReduceRunner :: reading AWSCredentials properties file...");
        AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
        System.out.println("ElasticMapReduceRunner :: accessKey = "+credentials.getAWSAccessKeyId());
        System.out.println("ElasticMapReduceRunner :: secretKey = "+credentials.getAWSSecretKey());

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://collocation-extraction-assignment/CollocationsExtractionUsingAmazonElasticMapReduce.jar")
                .withMainClass("main.CollocationExtraction")
                .withArgs(args[0], args[1], args[2], args[3], args[4]);

        StepConfig stepConfig = new StepConfig()
                .withName("CollocationExtraction")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(15)
                .withMasterInstanceType(InstanceType.M1Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M1Xlarge.toString())
                .withHadoopVersion("2.4.0").withEc2KeyName("hardwell")
                .withKeepJobFlowAliveWhenNoSteps(false);


        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withName("ExtractCollations")
                .withInstances(instances)
                .withAmiVersion("3.1.0")
                .withSteps(stepConfig)
                .withLogUri("s3n://collocation-extraction-assignment/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("ElasticMapReduceRunner :: successfully ran a job on Amazon Elastic Map Reduce");
        System.out.println("ElasticMapReduceRunner :: Ran job flow with id: " + jobFlowId);
        System.out.println("ElasticMapReduceRunner :: Ran job flow with class: " + runJobFlowResult.getClass());
    }

}
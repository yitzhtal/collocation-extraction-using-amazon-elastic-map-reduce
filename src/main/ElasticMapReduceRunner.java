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
        AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
        System.out.println(credentials.getAWSAccessKeyId());
        System.out.println(credentials.getAWSSecretKey());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://collocation-extraction-project/CollocationsExtractionUsingAmazonElasticMapReduce.jar")
                .withMainClass("CollocationExtraction")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data", "s3://collocation-extraction-project/output/finalOutput", args[0], args[1], args[2], args[3]);

        StepConfig stepConfig = new StepConfig()
                .withName("ExtractCollations")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(15)
                .withMasterInstanceType(InstanceType.M1Small.toString())
                .withSlaveInstanceType(InstanceType.M1Small.toString())
                .withHadoopVersion("2.7.3").withEc2KeyName("hardwell")
                .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-4.7.0")
                .withName("ExtractCollations")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://collocation-extraction-project/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}
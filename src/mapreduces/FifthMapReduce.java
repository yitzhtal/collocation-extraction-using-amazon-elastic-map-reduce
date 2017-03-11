package mapreduces;

/**
 * Created by Tal on 03/03/2017.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import corpus.Bigram;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuffer;
import corpus.CalculatedBigram;

//this map reduce calculates N

public class FifthMapReduce {

    public FifthMapReduce() {}

    public static class FifthMapReduceMapper extends Mapper<LongWritable, Text, CalculatedBigram, Text> {
        private Logger logger = Logger.getLogger(FifthMapReduceMapper.class);

        public FifthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text first = new Text(itr.nextToken());
            Text second = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text npmiAsText = new Text(itr.nextToken());

            CalculatedBigram bigram = new CalculatedBigram(first,second,decade,npmiAsText); //npmi has to be here so it will be sorted
            CalculatedBigram bigramByDecade = new CalculatedBigram(new Text("*"),new Text("*"),decade,new Text("~"));

            context.write(bigram,npmiAsText); //we write the data from the former map reduce
            context.write(bigramByDecade,npmiAsText);
            logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = " + value + ">");
            logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = *>");
        }
    }

    public static class FifthMapReducePartitioner extends Partitioner< CalculatedBigram, Text > {

        @Override
        public int getPartition(CalculatedBigram bigram, Text text, int numReduceTasks) {
                       return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                    }
         }

    public static class FifthMapReduceReducer extends Reducer<CalculatedBigram,Text,CalculatedBigram,Text> {
        private Logger logger = Logger.getLogger(FifthMapReduceMapper.class);
        private double sumOfAllNormalizedPMI;

        //keep track of the incoming keys
        private Text currentDecade;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            sumOfAllNormalizedPMI = 0;
            currentDecade = new Text("");
        }

        @Override
        public void reduce(CalculatedBigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            logger.info("------------------------");
            logger.info("Reducer :: Input :: <key = " + key.toString() + ",value="+values.toString()+">");

            if(!key.getDecade().equals(currentDecade)) {
                currentDecade = key.getDecade();
                sumOfAllNormalizedPMI = 0;
                double sum = 0;
                for (Text value : values) {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    sum += (double) Double.parseDouble(itr.nextToken());
                }
                sumOfAllNormalizedPMI += sum;
            } else {
                if (key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")) {
                    sumOfAllNormalizedPMI = 0;
                    double sum = 0;
                    for (Text value : values) {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        sum += (double) Double.parseDouble(itr.nextToken());
                    }
                    sumOfAllNormalizedPMI += sum;
                } else {
                    StringBuffer dataToTransfer = new StringBuffer("");
                    for (Text value : values) {
                        logger.info(" dataToTransfer += " +value.toString());
                        dataToTransfer.append(value.toString());
                    }

                    StringTokenizer itr = new StringTokenizer(dataToTransfer.toString());

                    //now we have  sumOfAllNormalizedPMI!
                    double npmi = Double.parseDouble(itr.nextToken());
                    Text npmiAsText = new Text(String.valueOf(npmi));
                    double sumOfAllNormalizedPMIasDouble = Double.parseDouble(String.valueOf(sumOfAllNormalizedPMI));

                    logger.info("npmi = "+npmi + ", sumOfAllNormalizedPMIasDouble= "+ sumOfAllNormalizedPMIasDouble);
                    double relativePMI = (double) npmi / sumOfAllNormalizedPMIasDouble;
                    Text relativePMIAsText = new Text(String.valueOf(relativePMI));
                    logger.info("relativePMI = " + relativePMI);

                    Double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
                    Double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));

                    logger.info("minPmi = "+minPmi +", relMinPmi = "+relMinPmi);
                    if(npmi >= minPmi && relativePMI >= relMinPmi) {
                        context.write(new CalculatedBigram(key.getFirst(),key.getSecond(),key.getDecade()), npmiAsText);
                        logger.info("Reducer :: Output :: <key = " + key.toString() + ",value = " + new Text(npmiAsText.toString() )+ ">");
                    } else {
                        logger.info("Reducer :: Output :: rejected the bigram " + key.toString() + "!");
                    }
                }
            }

            logger.info("------------------------");
        }
    }
}
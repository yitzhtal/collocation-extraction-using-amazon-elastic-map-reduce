package mapreduces;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import corpus.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.StringTokenizer;

//this map reduce is calculating c(W2)

public class ThirdMapReduce {

    public ThirdMapReduce() {}

    public static class ThirdMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {
        private Logger logger = Logger.getLogger(ThirdMapReduceMapper.class);

        public ThirdMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text first = new Text(itr.nextToken());
            Text second = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text numberOfOccurrences = new Text(itr.nextToken());
            Text Cw1 = new Text(itr.nextToken());
            Text dataToTransfer = new Text(numberOfOccurrences.toString() + " "+Cw1.toString());

            //we reverse it! (For the sorting)
            Bigram bigram = new Bigram(second,first,decade);
            Bigram bigramWithAsterisk = new Bigram(second,new Text("*"),decade);

            //we also pass the data from the former map reduce

            context.write(bigram,dataToTransfer);
            context.write(bigramWithAsterisk,dataToTransfer);
            logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = " + value + ">");
            logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = *>");
        }
    }

    public static class ThirdMapReducePartitioner extends Partitioner< Bigram, Text > {

        @Override
        public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
                        return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                   }
        }

    public static class ThirdMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {
        private Logger logger = Logger.getLogger(ThirdMapReduceMapper.class);
        private int secondWordCounter;

        //keep track of the incoming keys
        private Text currentSecondWord;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            secondWordCounter = 0;
            currentSecondWord = new Text("");
        }

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            logger.info("------------------------");
            logger.info("Reducer :: Input :: <key = " + key.toString() + ",value="+values.toString()+">");

            if(!key.getFirst().equals(currentSecondWord)) {
                currentSecondWord = key.getFirst();
                secondWordCounter = 0;
                int sum = 0;
                for (Text value : values) {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    sum += Integer.parseInt(itr.nextToken());
                }
                secondWordCounter += sum;
            } else {
                if (key.getSecond().toString().equals("*")) {
                    secondWordCounter = 0;
                    int sum = 0;
                    for (Text value : values) {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        sum += Integer.parseInt(itr.nextToken());
                    }
                    secondWordCounter += sum;
                } else {
                    String dataToTransfer = "";
                    for (Text value : values) {
                        dataToTransfer += value.toString();
                    }
                    Text Cw2 = new Text(String.valueOf(secondWordCounter));
                    context.write(new Bigram(key.getSecond(), key.getFirst(), key.getDecade()), new Text(dataToTransfer.toString() + " " + Cw2.toString()));
                }
            }
            logger.info("------------------------");
        }
    }
}
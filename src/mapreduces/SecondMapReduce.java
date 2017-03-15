package mapreduces;

import org.apache.hadoop.mapreduce.Partitioner;
import corpus.Bigram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.StringTokenizer;


//this map reduce is calculating C(w1)

public class SecondMapReduce {

    public SecondMapReduce() {}

    public static class SecondMapReduceMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {
        //private Logger logger = Logger.getLogger(SecondMapReduceMapper.class);

        public SecondMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text first = new Text(itr.nextToken());
            Text second = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text numberOfOccurrences = new Text(itr.nextToken());
            LongWritable numberOfOccurrencesToSend = new LongWritable(Integer.parseInt(numberOfOccurrences.toString()));
            Bigram bigram = new Bigram(first,second,decade);
            Bigram bigramWithAsterisk = new Bigram(first,new Text("*"),decade);

            context.write(bigram,numberOfOccurrencesToSend); //we write the data from the former map reduce
            context.write(bigramWithAsterisk,numberOfOccurrencesToSend);
            //logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = " + value + ">");
            //logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = *>");
        }
    }

    public static class SecondMapReducePartitioner extends Partitioner< Bigram, LongWritable > {

        @Override
        public int getPartition(Bigram bigram, LongWritable intWritable, int numReduceTasks) {
                        return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                    }
         }

    public static class SecondMapReduceReducer extends Reducer<Bigram,LongWritable,Bigram,Text> {
        //private Logger logger = Logger.getLogger(SecondMapReduceMapper.class);
        private long firstWordCounter;

        //keep track of the incoming keys
        private Text currentFirstWord;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
                firstWordCounter = 0;
                currentFirstWord = new Text("");
        }

        @Override
        public void reduce(Bigram key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            //logger.info("------------------------");
            //logger.info("Reducer :: Input :: <key = " + key.toString() + ",value="+values.toString()+">");
            //logger.info("first word = "+currentFirstWord);
            if(!key.getFirst().equals(currentFirstWord)) {
                //logger.info("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
                    currentFirstWord = key.getFirst();
                    //logger.info("currentFirstWord changed to = "+currentFirstWord);
                    firstWordCounter = 0;
                    long sum = 0;
                    for (LongWritable value : values) {
                        sum += value.get();
                    }
                    firstWordCounter += sum;
            } else {
                    if (key.getSecond().toString().equals("*")) {
                        //logger.info("first word = "+currentFirstWord);
                        //logger.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                        firstWordCounter = 0;
                        long sum = 0;
                        for (LongWritable value : values) {
                            sum += value.get();
                        }
                        firstWordCounter += sum;
                    } else {
                        //logger.info("first word = "+currentFirstWord);
                        Text Cw1w2 = new Text(values.iterator().next().toString());
                        Text Cw1 = new Text(String.valueOf(firstWordCounter));

                        context.write(new Bigram(key.getFirst(), key.getSecond(), key.getDecade()), new Text(Cw1w2.toString() + " " + Cw1.toString()));
                    }
            }
            //logger.info("------------------------");
        }
    }
}
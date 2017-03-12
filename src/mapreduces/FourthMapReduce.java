package mapreduces;

import corpus.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.StringTokenizer;

//this map reduce calculates N

public class FourthMapReduce {

    public FourthMapReduce() {}

    public static class FourthMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {
        //private Logger logger = Logger.getLogger(FourthMapReduceMapper.class);

        public FourthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text first = new Text(itr.nextToken());
            Text second = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text numberOfOccurrences = new Text(itr.nextToken());
            Text Cw1 = new Text(itr.nextToken());
            Text Cw2 = new Text(itr.nextToken());
            Text dataToTransfer = new Text(numberOfOccurrences.toString() + " "+Cw1.toString() + " " + Cw2.toString());

            Bigram bigram = new Bigram(first,second,decade);
            Bigram bigramByDecade = new Bigram(new Text("*"),new Text("*"), decade);

            context.write(bigram,dataToTransfer); //we write the data from the former map reduce
            context.write(bigramByDecade,dataToTransfer);
            //logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = " + value + ">");
            //logger.info("Mapper :: Output :: <key = " + bigram.toString() + ",value = *>");

        }
    }

    public static class FourthMapReducePartitioner extends Partitioner< Bigram, Text > {

        @Override
        public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
                        return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                    }
        }

    public static class FourthMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {
        //private Logger logger = Logger.getLogger(FourthMapReduceMapper.class);
        private int N;

        //keep track of the incoming keys
        private Text currentDecade;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            N = 0;
            currentDecade = new Text("");
        }

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            //logger.info("------------------------");
            //logger.info("Reducer :: Input :: <key = " + key.toString() + ",value="+values.toString()+">");

            if(!key.getDecade().equals(currentDecade)) {
                currentDecade = key.getDecade();
                N = 0;
                int sum = 0;
                for (Text value : values) {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    sum += Integer.parseInt(itr.nextToken());
                }
                N += sum;
            } else {
                if (key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")) {
                    N = 0;
                    int sum = 0;
                    for (Text value : values) {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        sum += Integer.parseInt(itr.nextToken());
                    }
                    N += sum;
                } else {
                    StringBuffer dataToTransfer = new StringBuffer("");
                    for (Text value : values) {
                        dataToTransfer.append(value.toString());
                    }

                    StringTokenizer itr = new StringTokenizer(dataToTransfer.toString());

                    //now we have N, we can calculate npmi!

                    double Cw1w2 = Double.parseDouble(itr.nextToken());
                    double Cw1 = Double.parseDouble(itr.nextToken());
                    double Cw2 = Double.parseDouble(itr.nextToken());
                    double NasDouble = Double.parseDouble(String.valueOf(N));
                    //logger.info("Cw1w2 = "+Cw1w2 + ", Cw1= "+ Cw1+ ",Cw2= "+Cw2+ ",N="+NasDouble);
                    double pmi = (double) (Math.log(Cw1w2) + Math.log(NasDouble) - Math.log(Cw1) - Math.log(Cw2));
                    //logger.info("pmi = " + pmi);
                    double pw1w2 = (double) Cw1w2 / NasDouble;
                    //logger.info("pw1w2 = "+pw1w2);
                    double formulaDenominator = ((-1) * Math.log(pw1w2));
                    //logger.info("formulaDenominator = "+ formulaDenominator);
                    double npmi = 0;
                    npmi = (double) pmi / formulaDenominator;
                    //logger.info("npmi = "+npmi);

                    Text npmiAsText = new Text(String.valueOf(npmi));
                    Double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
                    Double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));

                    //logger.info("minPmi = "+minPmi +", relMinPmi = "+relMinPmi);

                    context.write(new corpus.Bigram(key.getFirst(),key.getSecond(),key.getDecade()),npmiAsText);
                    //logger.info("Reducer :: Output :: <key = " + key.toString() + ",value = " + new Text("done!").toString() + ">");
                    //logger.info("Reducer :: Output :: <key = " + key.toString() + ",value = *>");
                }
            }

            //logger.info("------------------------");
        }
    }
}
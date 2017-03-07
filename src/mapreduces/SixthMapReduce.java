package mapreduces;

import corpus.CalculatedBigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Tal on 05/03/2017.
 */
public class SixthMapReduce {

    public SixthMapReduce() {}

    public static class SixthMapReduceMapper extends Mapper<LongWritable, Text, CalculatedBigram, Text> {
        private Logger logger = Logger.getLogger(SixthMapReduce.SixthMapReduceMapper.class);

        public SixthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
            StringTokenizer itr = new StringTokenizer(value.toString());
            context.write(new CalculatedBigram(new Text(itr.nextToken().toString()),new Text(itr.nextToken().toString()),
                    new Text(itr.nextToken().toString()),new Text(itr.nextToken().toString())),new Text(""));
            logger.info("Mapper :: Output :: <key = " + key.toString() + ",value = *>");
        }
    }
}

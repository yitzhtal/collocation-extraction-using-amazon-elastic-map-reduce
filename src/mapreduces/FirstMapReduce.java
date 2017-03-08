package mapreduces;


import corpus.Bigram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

// This map reduce is filtering stop words (eng/heb),
// and calculating C(w1,w2) for each pair by decade.

public class FirstMapReduce {

        public FirstMapReduce() {}

        public static class FirstMapReduceMapper extends Mapper<LongWritable, Text, Bigram, IntWritable> {
            private Logger logger = Logger.getLogger(FirstMapReduceMapper.class);

            final String[] EnglishStopWords = {"a","about","above","across","after","afterwards","again","against","all","almost","alone","along","already","also","although","always","am","among",
                    "amongst","amoungst","amount","an","interest","into","is","it","its","itself","keep","last","should","show","side","since","sincere","six","there","thereafter","thereby","therefore","therein"
                    ,"and","another","any","anyhow","anyone","anything","anyway","anywhere","are","around","as","at","back","be","became","because","become","becomes","becoming","been","before","thereupon","these",
                    "beforehand","behind","being","below","beside","besides","between","beyond","bill","both","bottom","but","by","call","can","cannot","cant","co","computer","con","could","couldnt","cry",
                    "de","describe","detail","do","done","down","due","during","each","eg","eight","either","eleven","else","elsewhere","empty","enough","etc","even","ever","every","everyone","everything"
                    ,"everywhere","except","few","fifteen","fify","fill","find","fire","first","five","for","former","formerly","forty","found","four","from","front","full","further","get","give","go","had"
                    ,"has","hasnt","have","he","hence","her","here","hereafter","hereby","herein","hereupon","hers","herself","him","himself","his","how","however","hundred","i","ie","if","in","inc","indeed"
                    ,"latter","latterly","least","less","ltd","made","many","may","me","meanwhile","might","mill","mine","more","moreover","most","mostly","move","much","must","my","myself","name"
                    ,"namely","neither","never","nevertheless","next","nine","no","nobody","none","noone","nor","not","nothing","now","nowhere","of","off","often","on","once","one","only","onto","or","other",
                    "others","otherwise","our","ours","ourselves","out","over","own","part","per","perhaps","please","put","rather","re","same","see","seem","seemed","seeming","seems","serious","several","she"
                    ,"sixty","so","some","somehow","someone","something","sometime","sometimes","somewhere","still","such","system","take","ten","than","that","the","their","them","themselves","then","thence"
                    ,"they","thick","thin","third","this","those","though","three","through","throughout","thru","thus","to","together","too","top","toward","towards","twelve","twenty","two","un","under","until"
                    ,"were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom",
                    "whose","why","will","with","within","without","would","yet","you","your","yours","yourself","yourselves","up","upon","us","very","via","was","we","well"};

            final String[] HebrewStopWords = {"של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה"
                    , "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
                    "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
                    "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
                    "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"",
                    "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע",
                    "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי",
                    "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
                    "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין",
                    "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"};

            boolean isStopWordsIncluded;

            public FirstMapReduceMapper() {
            }

            protected void setup(Context context) throws IOException, InterruptedException {
                isStopWordsIncluded = Integer.parseInt(context.getConfiguration().get("isStopWordsIncluded")) == 1? true : false;
            }

            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                logger.info("Mapper :: Input :: <key = " + key.toString() + ",value = " + value.toString() + ">");
                StringTokenizer itr = new StringTokenizer(value.toString());

                if(itr.countTokens() == 6) { //if it is our format - firstword secondword decade p1 p2 p3
                            boolean filterNumber = false;

                            String first_string = itr.nextToken();
                            if (first_string.contains("_")) first_string = first_string.substring(0, first_string.indexOf("_"));
                            Text first = new Text(first_string);

                            String second_string = itr.nextToken();
                            if (second_string.contains("_")) second_string = second_string.substring(0, second_string.indexOf("_"));
                            Text second = new Text(second_string);

                            if (first_string.matches(".*\\d+.*") || second_string.matches(".*\\d+.*")) {
                                filterNumber = true;
                            }

                            if(!filterNumber) {
                                            if (isStopWordsIncluded) {
                                                boolean exist = false;
                                                if (context.getConfiguration().get("language").equals("eng")) {
                                                    first_string = first_string.trim().toLowerCase();
                                                    second_string = second_string.trim().toLowerCase();
                                                    for (int i = 0; i < EnglishStopWords.length; i++) {
                                                        if (EnglishStopWords[i].equals(first_string) || EnglishStopWords[i].equals(second_string) || first_string.equals("") || second_string.equals("")) {
                                                            exist = true;
                                                            break;
                                                        }
                                                    }

                                                    if (!exist) {
                                                        Text decade = new Text(itr.nextToken().substring(0, 3));
                                                        Text numberOfOccurrences = new Text(itr.nextToken());
                                                        Bigram bigram = new Bigram(first, second, decade);
                                                        context.write(bigram, new IntWritable(Integer.parseInt(numberOfOccurrences.toString())));
                                                    } else {
                                                        logger.info("Mapper :: has just found a stop word! rejected ->" + first_string + " or " + second_string + "!");
                                                    }
                                                } else {
                                                    for (int i = 0; i < HebrewStopWords.length; i++) {
                                                        if (HebrewStopWords[i].equals(first_string) || HebrewStopWords[i].equals(second_string) || first_string.equals("") || second_string.equals("")) {
                                                            exist = true;
                                                            break;
                                                        }
                                                    }

                                                    if (!exist) {
                                                        Text decade = new Text(itr.nextToken().substring(0, 3));
                                                        Text numberOfOccurrences = new Text(itr.nextToken());
                                                        Bigram bigram = new Bigram(first, second, decade);
                                                        context.write(bigram, new IntWritable(Integer.parseInt(numberOfOccurrences.toString())));
                                                    } else {
                                                        logger.info("Mapper :: has just found a stop word! rejected ->" + first_string + " or " + second_string + "!");
                                                    }
                                                }
                                            } else {
                                                Text decade = new Text(itr.nextToken().substring(0, 3));
                                                Text numberOfOccurrences = new Text(itr.nextToken());
                                                Bigram bigram = new Bigram(first, second, decade);
                                                context.write(bigram, new IntWritable(Integer.parseInt(numberOfOccurrences.toString())));
                                            }
                            }
                        }
                    }
                }

            public static class FirstMapReduceReducer extends Reducer<Bigram,IntWritable,Bigram,IntWritable> {
                private Logger logger = Logger.getLogger(FirstMapReduce.FirstMapReduceMapper.class);

                @Override
                public void reduce(Bigram key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                    logger.info("------------------------");
                    logger.info("Reducer :: Input :: <key = " + key.toString() + ",value=" + values.toString() + ">");

                    int sum = 0;
                    for (IntWritable value : values) {
                        sum += value.get();
                    }

                    logger.info("Reducer :: Output :: <key = " + key.toString() + ",value = " + new IntWritable(sum).toString() + ">");
                    context.write(key, new IntWritable(sum));
                    logger.info("------------------------");
                }
            }
}

package corpus;

import org.apache.hadoop.io.Text;

/**
 * Created by Tal on 03/03/2017.
 */
public class CalculatedBigram extends Bigram {

    public CalculatedBigram(Text first, Text second, Text decade , Text npmi) {
        super(first,second,decade,npmi);
    }

    public CalculatedBigram(Text first, Text second, Text decade) {
        super(first,second,decade,new Text(""));
    }

    public CalculatedBigram() {
        super();
    }

    @Override
    public String toString() {
        return first + " " + second + " " + decade + " " + super.npmi;
    }

    @Override
    public int compareTo(Bigram tp) {
        if(decade.compareTo(tp.getDecade()) > 0) {
            return 1;
        } else if(decade.compareTo(tp.getDecade()) < 0) {
            return -1;
        } else {
                if(npmi.compareTo(tp.getNpmi()) < 0) {
                    return 1;
                } else if(npmi.compareTo(tp.getNpmi()) > 0) {
                    return -1;
                } else {
                    if(first.compareTo(tp.getFirst()) < 0) {
                        return 1;
                    } else if(first.compareTo(tp.getFirst()) > 0) {
                        return -1;
                    } else {
                        if(second.compareTo(tp.getSecond()) > 0) {
                            return 1;
                        } else if(second.compareTo(tp.getSecond()) < 0) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                }
        }
    }
}


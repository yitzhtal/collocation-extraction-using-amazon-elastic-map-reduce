package corpus;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Bigram implements WritableComparable<Bigram> {

    protected Text first;
    protected Text second;
    protected Text decade;
    protected Text npmi;

    public Bigram(Text first, Text second, Text decade,Text npmi) {
        set(first, second, decade,npmi);
    }

    public Bigram(Text first, Text second, Text decade) {
        set(first, second, decade,new Text(""));
    }

    public Bigram() {
        set(new Text(), new Text(), new Text(),new Text());
    }

    public Text getDecade() {
        return decade;
    }

    public Text getNpmi() {
        return npmi;
    }

    public void setNpmi(Text npmi) {
        this.npmi = npmi;
    }

    public void setDecade(Text decade) {
        this.decade = decade;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void set(Text first, Text second, Text decade,Text npmi) {
        this.first = first;
        this.second = second;
        this.decade = decade;
        this.npmi = npmi;
    }

    public Text toText() {
        return new Text(this.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        decade.readFields(in);
        npmi.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        decade.write(out);
        npmi.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second + " " + decade;
    }

    @Override
    public int compareTo(Bigram tp) {
        if(decade.compareTo(tp.getDecade()) > 0) {
            return 1;
        } else if(decade.compareTo(tp.getDecade()) < 0) {
            return -1;
        } else {
            if(first.compareTo(tp.getFirst()) > 0) {
                return 1;
            } else if(first.compareTo(tp.getFirst()) < 0) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bigram)) return false;

        Bigram bigram = (Bigram) o;

        if (getFirst() != null ? !getFirst().equals(bigram.getFirst()) : bigram.getFirst() != null) return false;
        if (getSecond() != null ? !getSecond().equals(bigram.getSecond()) : bigram.getSecond() != null) return false;
        return getDecade() != null ? getDecade().equals(bigram.getDecade()) : bigram.getDecade() == null;
    }
}
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class NYSE_example_B {   //changin here too the values from LongWritable to FloatWritable
    //    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text ticker = new Text();
        //        Changing from LogWritable to FloatWritable because for the floating values of trade_price
        private FloatWritable trade_price = new FloatWritable(1);//stock column

        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] tokens = line.split(",");
//            we parse the date
            if (key.get() < 15) { //according to the column names of csv file
                return;//byte offset of the file first row must be less than 20
            }
            /* we calculate the record year
             * according to a column we split the yar, and we take the last one, and we check if it
             * is 10 last years  according to the requirements*/
            int year = Integer.parseInt(tokens[2].split("/")[2]);

            if (year > 9) {

//               we change this  ticker.set(tokens[0]); to  ticker.set(tokens[1]);
                ticker.set(tokens[1]);
//              Changing  from   trade_price.set(Long.parseLong(tokens[2])); to   trade_price.set(Float.parseFloat(tokens[2]));
//                trade_price.set(Float.parseFloat(tokens[2])); index from 2 to 6
                trade_price.set(Float.parseFloat(tokens[4])); /* The only thing different from task A*/ 
                output.collect(ticker, trade_price);
            }
        }
    }

//   we change  public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
//            long MaxPrice = 0;
            float MaxPrice = 0;
            while (values.hasNext()) {
//                long next_price = values.next().get();
                float next_price = values.next().get();
                if (next_price > MaxPrice) MaxPrice = next_price;
            }
//            output.collect(key, new LongWritable(MaxPrice));
            output.collect(key, new FloatWritable(MaxPrice));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(NYSE_example_B.class);
        conf.setJobName("NYSE_B");
        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(LongWritable.class); changed to :
        conf.setOutputValueClass(FloatWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}


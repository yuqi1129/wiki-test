package projectone;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

/**`
 * flink-test
 * Created by yuqi
 * Date:17-1-21
 */
public class WordCount {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(WordCount.class);

    public static void main(String [] args){
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStream  = streamExecutionEnvironment.socketTextStream("localhost",9999)
                .flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        dataStream.print();
        try{
            streamExecutionEnvironment.execute("Window word count");
        }catch (Exception e){
            logger.error("when execute the function,we get logger ");
        }

    }

    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>> {

        public void flatMap(String sentence, Collector<Tuple2<String,Integer>> out){
            for (String word : sentence.split(" ")){
                out.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}

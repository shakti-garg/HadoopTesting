import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WordCountUtil.class)
public class WordCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {

        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();


        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws Exception {
        String inputString = "This map feature can be used when This map tasks";
        mapDriver.withInput(new LongWritable(), new Text(inputString));

        mapDriver.withOutput(new Text("This"), new IntWritable(1));
        mapDriver.withOutput(new Text("can"), new IntWritable(1));
        mapDriver.withOutput(new Text("be"), new IntWritable(1));
        mapDriver.withOutput(new Text("This"), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("This"), values);

        reduceDriver.withOutput(new Text("This"), new IntWritable(2));

        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws Exception {
        String inputString = "This map feature can be used when This map tasks";
        mapReduceDriver.withInput(new LongWritable(), new Text(inputString));

        mapReduceDriver.withOutput(new Text("This"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("be"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("can"), new IntWritable(1));

        mapReduceDriver.runTest();
    }

    @Test
    public void testMapperReducerWithPowermock() throws Exception {
        String inputString = "This map feature can be used when This map tasks";
        mapReduceDriver.withInput(new LongWritable(), new Text(inputString));

        mapReduceDriver.withOutput(new Text("This"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("be"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("can"), new IntWritable(1));

        PowerMockito.mockStatic(WordCountUtil.class);
        when(WordCountUtil.getStopList()).thenReturn(new ArrayList<String>(Arrays.asList("a","the","this","it","there","can","be")));

        mapReduceDriver.runTest();
    }

}


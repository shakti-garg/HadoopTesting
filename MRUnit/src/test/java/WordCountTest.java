import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WordCountUtil.class)
public class WordCountTest {


    @Before
    public void setUp() { }

    @Test
    public void testMapper() throws Exception {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        MapDriver<Object, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(mapper);

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
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);

        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("This"), values);

        reduceDriver.withOutput(new Text("This"), new IntWritable(2));

        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws Exception {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        WordCount.IntSumReducer combiner = new WordCount.IntSumReducer();

        MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
            MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setCombiner(combiner);

        String inputString = "This map feature can be used when This map tasks";
        mapReduceDriver.withInput(new LongWritable(), new Text(inputString));

        mapReduceDriver.withOutput(new Text("This"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("be"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("can"), new IntWritable(1));

        mapReduceDriver.runTest();
    }

    @Test
    public void testMapperReducerWithPowermock() throws Exception {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        WordCount.IntSumReducer combiner = new WordCount.IntSumReducer();

        MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
            MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setCombiner(combiner);String inputString = "This map feature can be used when This map tasks";
        mapReduceDriver.withInput(new LongWritable(), new Text(inputString));

        mapReduceDriver.withOutput(new Text("This"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("be"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("can"), new IntWritable(1));

        PowerMockito.mockStatic(WordCountUtil.class);
        when(WordCountUtil.getStopList()).thenReturn(new ArrayList<String>(Arrays.asList("a","the","this","it","there","can","be")));

        mapReduceDriver.runTest();
    }

    @Test
    public void testMapperWithMockedContex() throws IOException, InterruptedException {
      Mapper.Context mockedContext = mock(Mapper.Context.class);

      WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
      String inputString = "This map feature can be used when This map tasks";
      mapper.map(new LongWritable(), new Text(inputString), mockedContext);

      ArgumentCaptor<Text> mapperKey = ArgumentCaptor.forClass(Text.class);
      ArgumentCaptor<IntWritable> mapperValue = ArgumentCaptor.forClass(IntWritable.class);

      verify(mockedContext, times(4)).write(mapperKey.capture(), mapperValue.capture());

      List<Text> expectedMapperKeys = Arrays.asList(
          new Text[]{new Text("This"), new Text("can")
              , new Text("be"), new Text("This")});
      List<IntWritable> expectedMapperValues = Arrays.asList(
          new IntWritable[]{new IntWritable(1), new IntWritable(1),
              new IntWritable(1), new IntWritable(1)});

      Assert.assertThat("mapper keys don't match", mapperKey.getAllValues(),
          containsInAnyOrder(expectedMapperKeys.toArray()));
    }

}


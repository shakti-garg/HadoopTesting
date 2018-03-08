import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class WordCountTest {

  private SparkConf sparkConf;

  @Before
  public void setUp(){
    sparkConf = new SparkConf().setAppName("Testcase-1");
    sparkConf.setMaster("local[2]");
  }

  @Test
  public void testWordCount(){
    //setting input path to a directory on local filesystem
    String[] args = new String[]{"src/test/resources/test.txt"};

    List<Tuple2<String, Integer>> output = WordCount.countWords(sparkConf, args);

    List<Tuple2<String, Integer>> expectedOutput = new ArrayList<Tuple2<String, Integer>>();
    expectedOutput.add(new Tuple2<String, Integer>("this", 3));
    expectedOutput.add(new Tuple2<String, Integer>("on", 2));
    expectedOutput.add(new Tuple2<String, Integer>("is", 1));
    expectedOutput.add(new Tuple2<String, Integer>("day", 1));
    expectedOutput.add(new Tuple2<String, Integer>("demo", 1));
    expectedOutput.add(new Tuple2<String, Integer>("platform", 1));
    expectedOutput.add(new Tuple2<String, Integer>("great", 1));
    expectedOutput.add(new Tuple2<String, Integer>("sunny", 1));

    assertThat("List equality without order",
        output, containsInAnyOrder(expectedOutput.toArray()));
  }
}

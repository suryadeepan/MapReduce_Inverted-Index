import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class InvertedIndexMapReduceTest {

    private Mapper<Object, Text, Text, Text> mapper;
    private Reducer<Text, Text, Text, Text> reducer;
    private MapReduceDriver<Object, Text, Text, Text, Text, Text> driver;

    /**
     * Setup the mapper for inverted index.
     */
    @Before
    public void setUp() {
        mapper = new InvertedIndexMapper();
        reducer = new InvertedIndexReducer();
        driver = new MapReduceDriver<>(mapper, reducer);
    }

    @Test
    public void testInvertedIndexMapReduce() throws IOException {
        driver.withInput(new Text(""),
                new Text("bar hey whatever literally randomly typing whatever"))
                .withMapInputPath(new Path("s3://your-bucket/input/file-000001"))
                .withOutput(new Text("bar"), new Text("(file-000001)"))
                .withOutput(new Text("hey"), new Text("(file-000001)"))
                .withOutput(new Text("whatever"), new Text("(file-000001)"))
                .withOutput(new Text("literally"), new Text("(file-000001)"))
                .withOutput(new Text("typing"), new Text("(file-000001)"))
                .withOutput(new Text("randomly"), new Text("(file-000001)"))
                .runTest(false);
    }
}

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class InvertedIndexMapTest {
    private Mapper<Object, Text, Text, Text> mapper;
    private MapDriver<Object, Text, Text, Text> driver;

    @Before
    public void setUp() {
        mapper = new InvertedIndexMapper();
        driver = new MapDriver<>(mapper);
    }
    /**
     * {@code MapDriver.runTest(false)}: the order does not matter.
     * @throws IOException if io exception occurs
     */
    @Test
    public void testInvertedIndexMapper() throws IOException {
        Path inpuPath = new Path("s3://your_bucket/input/file-000001");
        driver.withMapInputPath(inpuPath)
                .withInput(new Text(""), new Text("bar hey whatever"))
                .withOutput(new Text("bar"), new Text("file-000001"))
                .withOutput(new Text("whatever"), new Text("file-000001"))
                .withOutput(new Text("hey"), new Text("file-000001"))
                .runTest(false);
    }
}

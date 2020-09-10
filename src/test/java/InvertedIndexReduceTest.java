import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class InvertedIndexReduceTest {
    private Reducer<Text, Text, Text, Text> reducer;
    private ReduceDriver<Text, Text, Text, Text> driver;
    /**
     * Setup the reducer for inverted index.
     */
    @Before
    public void setUp() {
        reducer = new InvertedIndexReducer();
        driver = new ReduceDriver<>(reducer);
    }

    /**
     * {@code ReduceDriver.runTest(false)}: the order does not matter.
     *
     * @throws IOException if io exception occurs
     */
    @Test
    public void testInvertedIndexReducer() throws IOException {
        driver.withInput(new Text("foo"), Arrays.asList(new Text("file-000000"), new Text("file-000001")))
                .withInput(new Text("bar"), Arrays.asList(new Text("file-000001"), new Text("file-000010"), new Text("file-000011")))
                .withOutput(new Text("bar"), new Text("(file-000001,file-000010,file-000011)"))
                .withOutput(new Text("foo"), new Text("(file-000000,file-000001)"))
                .runTest(false);
    }
}

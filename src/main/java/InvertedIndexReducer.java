import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.TreeSet;
import java.util.StringJoiner;
import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * The reduce function to run the inverted index job.
     * TODO: Implement the reduce method.
     * inputFiles: (filename1,filename2,...)
     * Output (word, inputFiles) key/value pair.
     * the file names should be in ascending lexicographical order
     * You may want to read the Javadoc of
     * {@link java.util.StringJoiner#StringJoiner(CharSequence, CharSequence, CharSequence)}
     * to construct strings with the required output format.
     *
     * @param key     input key of reducer
     * @param values  input values of reduce which is iterable
     * @param context output key/value pair of reducer
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> tree_Set = new TreeSet<String>();
        for (Text value : values) {
            tree_Set.add(value.toString());
        }
        StringJoiner sj = new StringJoiner(",","(",")");
        for (String value : tree_Set) {
            sj.add(value);
        }
        context.write(key, new Text(sj.toString()));
    }
}
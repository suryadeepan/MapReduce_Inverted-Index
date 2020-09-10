import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.StringTokenizer;
import java.io.IOException;

/**
 * Mapper Utility for inverted index.
 */
public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text title = new Text();
    /**
     * Map function for inverted index job.
     * TODO: Implement the map method.
     * Output (word, inputFileName) key/value pair.
     * Hint:
     * StringTokenizer is faster than String.split()
     * Use getInputFileName to get the name of the input file
     *
     * @param key     input key of mapper
     * @param value   input value of mapper
     * @param context output key/value pair of mapper
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tkn = new StringTokenizer(value.toString());
        title.set(getInputFileName(context));
        while (tkn.hasMoreTokens()) {
            //String str_word = tkn.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            word.set(tkn.nextToken());
            if(word.toString() != "" && !word.toString().isEmpty()){
                context.write(word, title);
            }
        }
    }

    /**
     * Utility method to get the input filename
     * @param context the context from map function
     * @return the input file name
     */
    private String getInputFileName(Context context) {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        return fileSplit.getPath().getName();
    }
}
package bmw;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;

import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;



public class CascadingWordCount extends Configured implements org.apache.hadoop.util.Tool {

    @SuppressWarnings("rawtypes")
    public final static void main(String[] args) throws Exception {
        CascadingWordCount app = new CascadingWordCount();
        int ret = ToolRunner.run(new Configuration(), app, args);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        // Define source and sink Taps.
        Scheme sourceScheme = new TextLine(new Fields("line"));
        Tap source = new Hfs(sourceScheme, inputPath);

        Scheme sinkScheme = new TextLine();
        Tap sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);

        // the 'head' of the pipe assembly
        Pipe assembly = new Pipe("wordcount");

        // For each input Tuple
        // parse out each word into a new Tuple with the field name "word"
        // regular expressions are optional in Cascading
        assembly = new Each(assembly, new Fields("line"), new Tokenizer(new Fields("word")));

        // For every Tuple group
        // count the number of occurrences of "word" and store result in
        // a field named "count"
        assembly = new CountBy(assembly, new Fields("word"), new Fields("count"), 5000000);

        Properties properties = new Properties();
        Iterator<Map.Entry<String, String>> iterator = getConf().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        AppProps.setApplicationJarClass(properties, CascadingWordCount.class);
        System.out.println();
        FlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect("word-count", source, sink, assembly);

        // execute the flow, block until complete
        flow.complete();
        return 0;
    }
}


class Tokenizer extends BaseOperation implements Function {

    public Tokenizer(Fields declaredFields) {
        super(declaredFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String line = (String) arguments.getObject(0);
        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()) {
            functionCall.getOutputCollector().add(new Tuple(tokenizer.nextToken()));
        }
    }
}

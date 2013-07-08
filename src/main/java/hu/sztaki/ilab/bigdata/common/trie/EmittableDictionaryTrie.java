/**
 * EmittableDictionaryTrie.java
 * Extended Dictionary trie which is able to emit its content to reducers
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.trie;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author garzo
 */
public class EmittableDictionaryTrie<Context extends TaskInputOutputContext> extends DictionaryTrie {

    private Context context;
    private VLongWritable count = new VLongWritable();
    protected char prefixChar;
    protected EmitMode emitMode = EmitMode.EMIT_LABEL;
    private static final Log LOG = LogFactory.getLog(EmittableDictionaryTrie.class);

    public EmittableDictionaryTrie() {
    
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public void setPrefixChar(char c) {
        this.prefixChar = c;
    }

    public void setEmitMode(EmitMode mode) {
        this.emitMode = mode;
    }

    @Override
    protected void emit(String node, DictionaryTrie trie) {
        try {
            Text key = new Text(prefixChar + node);
            if (emitMode == EmitMode.EMIT_ONE) {
                count.set(1);
            } else if (emitMode == EmitMode.EMIT_LABEL) {
                count.set(trie.label);
            }
            context.write(key, count);
        } catch (Exception ex) {
            LOG.debug(ex.getMessage());
        }
    }

}

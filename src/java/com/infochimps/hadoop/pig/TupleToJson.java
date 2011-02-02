package com.infochimps.hadoop.pig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TupleToJson extends EvalFunc<String>
{
    protected Schema schema_;
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;
        String[] fieldNames  = input.get(input.size()-1).toString().split(",");
        StringBuffer jsonBuf = new StringBuffer();
        jsonBuf.append("{\"");
        for(int i = 0; i < input.size()-1; i++) {
            try {
                
                if( i != 0 && i != input.size()-2) {
		    jsonBuf.append(",");
		}
                
                byte type = DataType.findType(input.get(i));
                switch (type) {
                    case DataType.BYTEARRAY:
                    case DataType.CHARARRAY:
                        jsonBuf.append(fieldNames[i]);
                        jsonBuf.append("\":\"");
                        jsonBuf.append(input.get(i));
                        jsonBuf.append("\"");
                        break;
                    case DataType.DOUBLE:
                    case DataType.FLOAT:
                    case DataType.INTEGER:
                    case DataType.LONG:
                        jsonBuf.append(fieldNames[i]);
                        jsonBuf.append("\":");
                        jsonBuf.append(input.get(i));
                        break;
                    case DataType.NULL: break;
                }

            } catch (Exception e) {
                log.warn("Failed to process input; error - " + e.getMessage());
                return null;
            }
        }
        jsonBuf.append("}");
        return jsonBuf.toString();
    }
}

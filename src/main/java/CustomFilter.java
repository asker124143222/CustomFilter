import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

import java.io.IOException;


/**
 * @Author: xu.dm
 * @Date: 2019/4/14 12:16
 * @Description: 自定义过滤器，选择列值匹配的行数据
 */
public class CustomFilter extends FilterBase {
    private byte[] value = null;
    private boolean filterRow = true;

    public CustomFilter() {
        super();
    }

    public CustomFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public void reset() throws IOException {
        this.filterRow = true;
    }


    @Override
    public boolean filterRow() throws IOException {
        return this.filterRow;
    }


    //匹配的数据不过滤
    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
        if(CellUtil.matchingValue(c,value))
            filterRow = false;
        return ReturnCode.INCLUDE;
    }

    /**
     * protobuf生成MyFilterProtos
     */
    @Override
    public byte[] toByteArray() throws IOException {
        MyFilterProtos.CustomFilter.Builder builder = MyFilterProtos.CustomFilter.newBuilder();
        if(value!=null)
            builder.setValue(ByteStringer.wrap(value));

        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        MyFilterProtos.CustomFilter proto;
        try {
            proto = MyFilterProtos.CustomFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new CustomFilter(proto.getValue().toByteArray());
    }
}

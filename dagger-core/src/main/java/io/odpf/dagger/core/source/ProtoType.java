package io.odpf.dagger.core.source;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.core.protohandler.TypeInformationFactory;
import io.odpf.dagger.core.utils.Constants;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Getting TypeInformation required for Flink from the proto.
 */
public class ProtoType implements Serializable {

    private transient Descriptor protoFieldDescriptor;
    private String protoClassName;
    private String rowtimeAttributeName;
    private StencilClientOrchestrator stencilClientOrchestrator;

    /**
     * Instantiates a new Proto type.
     *
     * @param protoClassName            the proto class name
     * @param rowtimeAttributeName      the rowtime attribute name
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ProtoType(String protoClassName, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.protoClassName = protoClassName;
        this.rowtimeAttributeName = rowtimeAttributeName;
    }

    /**
     * Gets row type info.
     *
     * @return the row type info
     */
    public TypeInformation<Row> getRowType() {
        TypeInformation<Row> rowNamed = TypeInformationFactory.getRowType(getProtoFieldDescriptor());
        RowTypeInfo rowTypeInfo = (RowTypeInfo) rowNamed;
        ArrayList<String> fieldNames = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldNames()));
        ArrayList<TypeInformation> fieldTypes = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldTypes()));
        fieldNames.add(Constants.INTERNAL_VALIDATION_FILED_KEY);
        fieldTypes.add(Types.BOOLEAN);
        fieldNames.add(rowtimeAttributeName);

//        DataType dataType = DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);
//        fieldTypes.add(dataType);
//        DataType timestamp = DataTypes.TIMESTAMP(3);

        fieldTypes.add(Types.SQL_TIMESTAMP);
        return Types.ROW_NAMED(fieldNames.toArray(new String[0]), fieldTypes.toArray(new TypeInformation[0]));
    }

//    public TypeInformation<Row> getRowType() {
//        TypeInformation<Row> rowNamed = TypeInformationFactory.getRowType(getProtoFieldDescriptor());
//        RowTypeInfo rowTypeInfo = (RowTypeInfo) rowNamed;
//        ArrayList<String> fieldNames = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldNames()));
//        ArrayList<TypeInformation> fieldTypes = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldTypes()));
//        fieldNames.add(Constants.INTERNAL_VALIDATION_FILED_KEY);
//        fieldTypes.add(Types.BOOLEAN);
//        fieldNames.add(rowtimeAttributeName);
////        DataType dataType = DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);
////        fieldTypes.add(dataType);
//        DataType timestamp = DataTypes.TIMESTAMP(3);
//
//        fieldTypes.add(Types.SQL_TIMESTAMP);
//        return Types.ROW_NAMED(fieldNames.toArray(new String[0]), fieldTypes.toArray(new TypeInformation[0]));
//    }

    private Descriptor getProtoFieldDescriptor() {
        if (protoFieldDescriptor == null) {
            protoFieldDescriptor = createFieldDescriptor();
        }
        return protoFieldDescriptor;
    }

    private Descriptor createFieldDescriptor() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }
}

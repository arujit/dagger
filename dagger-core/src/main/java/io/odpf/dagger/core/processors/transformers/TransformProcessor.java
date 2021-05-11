package io.odpf.dagger.core.processors.transformers;

import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.PreProcessorConfig;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.types.Preprocessor;
import org.apache.flink.configuration.Configuration;

import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.exception.TransformClassNotDefinedException;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.utils.Constants;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformProcessor implements Preprocessor, PostProcessor, TelemetryPublisher {
    protected final List<TransformConfig> transformConfigs;

    public String getTableName() {
        return tableName;
    }

    protected final String tableName;
    private final Configuration configuration;
    private final Map<String, List<String>> metrics = new HashMap<>();
    protected final TelemetryTypes type;

    public TransformProcessor(List<TransformConfig> transformConfigs, Configuration configuration) {
        this("NULL", TelemetryTypes.POST_PROCESSOR_TYPE, transformConfigs, configuration);
    }

    public TransformProcessor(String tableName, TelemetryTypes type, List<TransformConfig> transformConfigs, Configuration configuration) {
        this.transformConfigs = transformConfigs == null ? new ArrayList<>() : transformConfigs;
        this.configuration = configuration;
        this.tableName = tableName;
        this.type = type;
        TransformerUtils.populateDefaultArguments(this);
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (TransformConfig transformConfig : transformConfigs) {
            String className = transformConfig.getTransformationClass();
            try {
                Transformer function = getTransformMethod(transformConfig, className, streamInfo.getColumnNames());
                streamInfo = function.transform(streamInfo);
            } catch (ReflectiveOperationException e) {
                throw new TransformClassNotDefinedException(e.getMessage());
            }
        }
        return streamInfo;
    }

    @Override
    public boolean canProcess(PreProcessorConfig processorConfig) {
        return processorConfig.getTableTransformers().stream().anyMatch(x -> x.tableName.equals(this.tableName));
    }

    @Override
    public boolean canProcess(PostProcessorConfig processorConfig) {
        return processorConfig.hasTransformConfigs();
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        switch (this.type) {
            case POST_PROCESSOR_TYPE:
                addMetric(type.getValue(), Constants.TRANSFORM_PROCESSOR);
                break;
            case PRE_PROCESSOR_TYPE:
                addMetric(type.getValue(), this.tableName + "_" + Constants.TRANSFORM_PROCESSOR);
                break;
            default:
                break;
        }
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class, String[].class, Configuration.class);
        return (Transformer) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments(), columnNames, configuration);
    }
}

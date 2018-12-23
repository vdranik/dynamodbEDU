package com.vdranik.dynamodb.streams;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class StreamsRecordProcessorFactory implements  IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new StreamRecordsProcessor();
    }
}

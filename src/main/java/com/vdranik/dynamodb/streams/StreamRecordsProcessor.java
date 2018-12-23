package com.vdranik.dynamodb.streams;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.util.Map;

public class StreamRecordsProcessor implements IRecordProcessor {


    @Override
    public void initialize(InitializationInput initializationInput) {
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {
            if (record instanceof RecordAdapter) {
                com.amazonaws.services.dynamodbv2.model.Record streamRecord =
                        ((RecordAdapter) record).getInternalObject();

                if ("INSERT".equals(streamRecord.getEventName())) {
                    Map<String, AttributeValue> values = streamRecord
                            .getDynamodb()
                            .getNewImage();

                    int totalPrice =
                            Integer.parseInt(values.get("totalPrice").getN());

                    if (totalPrice > 900) {
                        System.out.println("Expensive order: " + values);
                    }
                }
            }
            checkpoint(processRecordsInput.getCheckpointer());
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }
}

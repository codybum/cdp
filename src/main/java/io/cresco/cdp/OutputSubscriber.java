package io.cresco.cdp;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private Schema schema;
    private String topic;
    private String streamName;
    private DatumReader<GenericData.Record> reader;

    public OutputSubscriber(Schema schema, String topic, String streamName) {
        this.schema = schema;
        this.topic = topic;
        this.streamName = streamName;
        reader = new GenericDatumReader<>(schema);
    }

    @Override
    public void onMessage(Object msg) {

        try {
            ByteBuffer bb = (ByteBuffer) msg;
            Decoder decoder = new DecoderFactory().binaryDecoder(bb.array(), null);
            GenericData.Record rec = reader.read(null, decoder);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
            DatumWriter<GenericData.Record> writer = new ReflectDatumWriter<>(schema);
            writer.write(rec, encoder);
            encoder.flush();

            String input = new String(outputStream.toByteArray());
            System.out.println("Original Object Schema JSON: " + schema);
            System.out.println("Original Object DATA JSON: "+ input);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}

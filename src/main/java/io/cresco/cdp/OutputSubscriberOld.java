package io.cresco.cdp;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class OutputSubscriberOld implements InMemoryBroker.Subscriber {

    private PluginBuilder plugin;
    private CLogger logger;

    private Schema schema;
    private String topic;
    private String streamName;
    private DatumReader<GenericData.Record> reader;



    private List<Map<String,String>> outputList;
    //private String outputRegion;
    //private String outputAgent;
    //private String outputPlugin;

    public OutputSubscriberOld(PluginBuilder pluginBuilder, Schema schema, String topic, String streamName, String outputListString) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(OutputSubscriberOld.class.getName(),CLogger.Level.Info);

        this.schema = schema;
        this.topic = topic;
        this.streamName = streamName;

        this.outputList = new Gson().fromJson(outputListString,new TypeToken<List<Map<String, String>>>() {
            }.getType());

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
           // System.out.println("Original Object Schema JSON: " + schema);
           // System.out.println("Original Object DATA JSON: "+ input);

            logger.info("CDP Output " + input);

            if(!outputList.isEmpty()) {

                for(Map<String,String> outputMap : outputList) {

                    String outputRegion = outputMap.get("region");
                    String outputAgent = outputMap.get("agent");
                    String outputPlugin = outputMap.get("pluginid");

                    logger.info("Sending out " + outputRegion + " " + outputAgent + " " + outputPlugin + " " + input);

                    MsgEvent inputMsg = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, outputRegion, outputAgent, outputPlugin);
                    inputMsg.setParam("action", "queryinput");
                    inputMsg.setParam("input_stream_name", streamName);
                    inputMsg.setCompressedParam("input_stream_payload", input);
                    plugin.msgOut(inputMsg);
                }

            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}

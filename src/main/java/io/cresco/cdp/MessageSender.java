package io.cresco.cdp;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.app.gEdge;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageSender implements Runnable  {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private boolean isInitQuery = false;


    public MessageSender(PluginBuilder plugin) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.gson = new Gson();

    }

    private boolean initQuery() {

        logger.info("STARTING PLUGIN: " + plugin.getPluginID());

        //create message to create query

        String inputRecordSchemaString = "{\"type\":\"record\",\"name\":\"Ticker\",\"fields\":[{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"urn\",\"type\":\"string\"},{\"name\":\"metric\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}]}";
        String inputStreamName = "UserStream";

        String outputStreamName = "BarStream";
        String outputStreamAttributesString = "source string, avgValue double";

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from UserStream#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into BarStream; ";

        //create message to self
        MsgEvent createQuery = plugin.getPluginMsgEvent(MsgEvent.Type.CONFIG,plugin.getPluginID());
        createQuery.setParam("action","queryadd");
        createQuery.setParam("input_schema",inputRecordSchemaString);
        createQuery.setParam("input_stream_name",inputStreamName);
        createQuery.setParam("output_stream_name", outputStreamName);
        createQuery.setParam("output_stream_attributes", outputStreamAttributesString);
        createQuery.setParam("query",queryString);

        System.out.println("SENDING CREATE QUERY");
        MsgEvent createQueryReturn = plugin.sendRPC(createQuery);
        String outputSchema = createQueryReturn.getParam("output_schema");
        System.out.println("CreateQueryReturn output_schema: " + outputSchema);

        return true;
    }


    public void run() {




        while(plugin.isActive()) {
            try {

                if(isInitQuery) {
                    logger.info("LOOP: " + plugin.getPluginID());

                    String inputStreamName = "UserStream";

                    MsgEvent inputMsg = plugin.getPluginMsgEvent(MsgEvent.Type.EXEC, plugin.getPluginID());
                    inputMsg.setParam("action", "queryinput");
                    inputMsg.setParam("input_stream_name", inputStreamName);
                    plugin.msgOut(inputMsg);

                } else {

                    isInitQuery = initQuery();

                }

                Thread.sleep(5000);
            } catch(Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logger.error("MessageSender: " + errors.toString());
            }
        }

        logger.debug("ENDING PLUGIN: " + plugin.getPluginID());
    }


}

package io.cresco.cdp;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private CEPEngine cep;

    public ExecutorImpl(PluginBuilder pluginBuilder, CEPEngine cep) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(),CLogger.Level.Info);
        this.cep = cep;
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "queryadd":
                    return addCEPQuery(incoming);
                case "querydel":
                    cep.clear();
                    incoming.setParam("iscleared",Boolean.TRUE.toString());
                    return incoming;

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        logger.info("INCOMING INFO MESSAGE : " + incoming.getParams());
        //System.out.println("INCOMING INFO MESSAGE FOR PLUGIN");
        return null;
    }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        //logger.info("INCOMING EXEC MESSAGE : " + incoming.getParams());

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "queryinput":
                    queryInput(incoming);
                    break;

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }


    public MsgEvent addCEPQuery(MsgEvent incoming) {


        System.out.println("ADD QUERY : " + incoming.getParams().toString());
        cep.createCEP(
        incoming.getParam("input_schema"),
        incoming.getParam("input_stream_name"),
        incoming.getParam("output_stream_name"),
        incoming.getParam("output_stream_attributes"),
        incoming.getParam("query"));

        //remove body
        incoming.removeParam("input_schema");
        incoming.removeParam("input_stream_name");
        //incoming.removeParam("output_stream_name");
        incoming.removeParam("output_stream_attributes");
        incoming.removeParam("query");

        System.out.println("ADD QUERY RETURN 2");

        incoming.setParam("output_schema",cep.getSchema(incoming.getParam("output_stream_name")).toString());

        System.out.println("ADD QUERY RETURN 1");

        return incoming;
    }

    public void queryInput(MsgEvent incoming) {
        System.out.println("INCOMING: " + incoming.getParams().toString());
        //cep.input(incoming.getParam("input_stream_name"), cep.getStringPayload());
        cep.input(incoming.getParam("input_stream_name"), incoming.getParam("input_stream_payload"));


    }

}
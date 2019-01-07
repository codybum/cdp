package io.cresco.cdp;


import io.cresco.library.agent.AgentService;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.*;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

@Component(
        service = { PluginService.class },
        scope=ServiceScope.PROTOTYPE,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        servicefactory = true,
        reference=@Reference(name="io.cresco.library.agent.AgentService", service=AgentService.class)
)

public class Plugin implements PluginService {

    public BundleContext context;
    public PluginBuilder pluginBuilder;
    private Executor executor;
    private CLogger logger;
    private Map<String, Object> map;
    public String myname;
    public CEPEngine cep;
    private Thread messageSenderThread = null;

    @Activate
    void activate(BundleContext context, Map<String, Object> map) {

        this.context = context;
        this.map = map;
        myname = "this is my name";

    }


    @Modified
    void modified(BundleContext context, Map<String, Object> map) {
        System.out.println("Modified Config Map PluginID:" + (String) map.get("pluginID"));
    }

    @Override
    public boolean inMsg(MsgEvent incoming) {

        pluginBuilder.msgIn(incoming);
        return true;
    }

    @Deactivate
    void deactivate(BundleContext context, Map<String,Object> map) {

        isStopped();
        this.context = null;
        this.map = null;

    }

    @Override
    public boolean isStarted() {


        try {
            pluginBuilder = new PluginBuilder(this.getClass().getName(), context, map);
            this.logger = pluginBuilder.getLogger(Plugin.class.getName(), CLogger.Level.Info);

            logger.info("STARTING CEP");
            this.cep = new CEPEngine(pluginBuilder);

            this.executor = new ExecutorImpl(pluginBuilder, cep);
            pluginBuilder.setExecutor(executor);

            while (!pluginBuilder.getAgentService().getAgentState().isActive()) {
                logger.info("Plugin " + pluginBuilder.getPluginID() + " waiting on Agent Init");
                Thread.sleep(1000);
            }



            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {


                        if (msg instanceof TextMessage) {

                            //System.out.println(RXQueueName + " msg:" + ((TextMessage) msg).getText());
                            String message = ((TextMessage) msg).getText();
                            logger.error("YES!!! " + message);

                        }
                    } catch(Exception ex) {

                        ex.printStackTrace();
                    }
                }
            };
            pluginBuilder.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,null);

            TextMessage tm = pluginBuilder.getAgentService().getDataPlaneService().createTextMessage();
            tm.setText("DID IT WORK?");

            MapMessage mapMessage = pluginBuilder.getAgentService().getDataPlaneService().createMapMessage();

            pluginBuilder.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,tm);

            //pluginBuilder.getAgentService().


            //pluginBuilder.getAgentService().getDataPlaneService().
            //releaseYear
            //TextMessage tm = sess.createTextMessage(message);
            //tm.setIntProperty("releaseYear",1977);

            /*
            MessageSender messageSender = new MessageSender(pluginBuilder);
            messageSenderThread = new Thread(messageSender);
            messageSenderThread.start();
            */

            pluginBuilder.setIsActive(true);

            logger.info("Started CDP PluginID:" + (String) map.get("pluginID"));

            return true;

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean isStopped() {
        try {

            pluginBuilder.setIsActive(false);

            logger.debug("PRE JOIN! " + pluginBuilder.getPluginID() + " isActive:" + pluginBuilder.isActive());
            if(messageSenderThread != null) {
                messageSenderThread.join();
            }
            logger.debug("POST JOIN! " + pluginBuilder.getPluginID() + " isActive:" + pluginBuilder.isActive());

            if(cep != null) {
                cep.shutdown();
            }

            return true;

        } catch(Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("isStopped: " + errors.toString());
            return false;
        }

    }
}
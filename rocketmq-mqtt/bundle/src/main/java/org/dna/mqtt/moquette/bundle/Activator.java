/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.dna.mqtt.moquette.bundle;

import java.io.File;
import java.text.ParseException;
import java.util.Properties;

import org.dna.mqtt.moquette.server.ConfigurationParser;
import org.dna.mqtt.moquette.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start and stop a configured version of the MQTT broker server.
 *
 * @author Didier Donsez
 * @todo add config admin for port (in NettyAcceptor)
 */
public class Activator{
    private static final Logger LOG = LoggerFactory.getLogger(Activator.class);
    
    MQUPConsumer mqupConsumer;
    public void start() throws Exception {
    	/*Server s=new Server();
    	s.startServer();*/
    	String configPath = System.getProperty("moquette.path", null);
        ConfigurationParser confParser = new ConfigurationParser();
        try {
            confParser.parse(new File(configPath, "config/moquette.conf"));
        } catch (ParseException pex) {
            LOG.warn("An error occurred in parsing configuration, fallback on default configuration", pex);
        }
        Properties configProps = confParser.getProperties();
        
        mqupConsumer = new MQUPConsumer();
        mqupConsumer.init(configProps);
        LOG.trace("Moquette MQTT Processor started, version 0.7-SNAPSHOT");
    }

    public void stop() throws Exception {
        LOG.info("Moquette MQTT Processor stopped, version 0.7-SNAPSHOT");
    }
    public static void main(String[] args) throws Exception{
    	new Activator().start();
    }
}

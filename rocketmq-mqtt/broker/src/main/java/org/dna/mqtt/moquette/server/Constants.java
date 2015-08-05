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
package org.dna.mqtt.moquette.server;

/**
 * Server constants keeper
 */
public class Constants {
    public static final String ATTR_CLIENTID = "ClientID";
    public static final String CLEAN_SESSION = "cleanSession";
    public static final String KEEP_ALIVE = "keepAlive";
    
    public static final String TOPIC_UPG_PATTERN="MQTT-UP-TG-%d";
    public static final String TOPIC_DOWNG_PATTERN="MQTT-DOWN-TG-%d";
    public static final String TOPIC_SYS_PATTERN="MQTT-SYS-%d";
    public static final String TOPIC_P2P="P/?";
    public static final String TOPIC_GROUP_PREFIX="G/";
    public static final String TOPIC_GROUP_LIMIT_PREFIX="G/LIMIT/";
    public static final String TOPIC_SYS_PREFIX="S/";
    public static final String TOPIC_PERSONAL_PREFIX="P/";
}

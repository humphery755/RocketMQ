package org.dna.mqtt.moquette.bundle;


import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupNotifyService extends ServiceThread {
	private static final Logger LOG = LoggerFactory.getLogger(GroupNotifyService.class);
	private final ConcurrentHashMap<String, Long> topicOffset = new ConcurrentHashMap();
	private HttpClient httpClient;
	private String mqttconsoleUrl;
	public GroupNotifyService(String mqttcmUrl){
		mqttconsoleUrl=mqttcmUrl;
	}
	
	public void init() {
		int maxTotal = 2000;
		int maxPerRoute = 20;
		int soTimeout = 1000;
		PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
		SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).setSoTimeout(soTimeout).setSoKeepAlive(false).setSoReuseAddress(true)
				.build();
		connManager.setDefaultSocketConfig(socketConfig);
		// Create connection configuration
		ConnectionConfig connectionConfig = ConnectionConfig.custom().setMalformedInputAction(CodingErrorAction.IGNORE)
				.setUnmappableInputAction(CodingErrorAction.IGNORE).setCharset(Consts.UTF_8)/*
																							 * .
																							 * setMessageConstraints
																							 * (
																							 * messageConstraints
																							 * )
																							 */.build();
		connManager.setDefaultConnectionConfig(connectionConfig);
		connManager.setMaxTotal(maxTotal);
		connManager.setDefaultMaxPerRoute(maxPerRoute);
		httpClient = HttpClientBuilder.create().setConnectionManager(connManager).build();
	}
	public void start() {
		super.start();
		
	}
	
	public void destroy() {
		try {
			httpClient.getConnectionManager().shutdown();
		} catch (Exception e) {
			LOG.error("", e);
		}
	}
	public boolean putRequest(final String topic,Long offset) {
		Long oldOffset = topicOffset.get(topic);
		if(oldOffset==null){
			oldOffset = topicOffset.putIfAbsent(topic,offset);
			if(oldOffset==null)oldOffset=offset;
		}
		synchronized (topicOffset) {
			if(oldOffset<offset){
				topicOffset.put(topic, offset);
			}
		}
		wakeup();
		return true;
	}

	private boolean doNotify() {
		if (topicOffset.isEmpty())
			return false;
		
		Iterator<Map.Entry<String, Long>> it = topicOffset.entrySet().iterator();  
		StringBuilder sb=new StringBuilder();
        while(it.hasNext()){  
            Map.Entry<String, Long> entry=it.next();
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
            //if(StringUtils.startsWith(entry.getKey(), "G/LIMIT/"))
            	it.remove();        //OK 
        }
        send(sb.toString());
		return true;
	}

	public void run() {
		LOG.info(this.getServiceName() + " service started");

		while (!this.isStoped()) {
			try {
				if (!this.doNotify())
					this.waitForRunning(0);
			} catch (Exception e) {
				LOG.error(this.getServiceName() + " service has exception. ", e);
			}
		}

		LOG.info(this.getServiceName() + " service end");
	}

	private boolean send(String data) {
		HttpPut request = new HttpPut(mqttconsoleUrl);
		try {
			StringEntity se=new StringEntity(data,ContentType.APPLICATION_FORM_URLENCODED);
			request.setEntity(se);
			HttpResponse response = httpClient.execute(request);
			String content = EntityUtils.toString(response.getEntity());
			if (response.getStatusLine().getStatusCode() == 200 && StringUtils.startsWith(content, "success")) {
				return true;
			}
		} catch (IOException e) {
			LOG.error("回调异常{}", data, e);
			return false;
		} finally {
			request.releaseConnection();
		}
		return false;
	}
	
	@Override
	protected void onWaitEnd() {	}

	@Override
	public String getServiceName() {		return GroupNotifyService.class.getSimpleName();		}
}

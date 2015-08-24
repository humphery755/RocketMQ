package org.dna.mqtt.moquette.bundle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class MQTTUPConsumer implements EventHandler<ByteBuf> {
	private static final Logger LOG = LoggerFactory.getLogger(MQTTUPConsumer.class);
	private SSDB ssdb;
	private RingBuffer<ByteBuf> m_ringBuffer;
	private ExecutorService m_executor;
	private boolean running;
	private GroupNotifyService groupNotifyService;
	private final Map<Byte, MqttProcessor> m_decoderMap = new HashMap<Byte, MqttProcessor>();
	private HttpClient httpClient;
	private String mqttconsoleUrl;
	private Properties configProps;
	private Integer brokergid;
	private DefaultMQProducer customEvtMqProducer;
	
	public MQTTUPConsumer(Properties configProps) {
		this.configProps = configProps;
	}

	public void init() {
		String ssdbIp = configProps.getProperty("ssdb.ip");
		int ssdbPort = Integer.valueOf(configProps.getProperty("ssdb.port"));
		mqttconsoleUrl = configProps.getProperty("mqtt.broker.url");
		brokergid = Integer.valueOf(configProps.getProperty("brokergid"));
		
		ThreadFactory disruptorThreadFactory = new ThreadFactory() {
			private AtomicInteger threadIndex = new AtomicInteger(0);

			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("MQTTUPConsumer.Disruptor.executor%d", this.threadIndex.incrementAndGet()));
			}
		};

		m_executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1, disruptorThreadFactory);
		int ringBufferSize = 1024;
		Disruptor<ByteBuf> disruptor = new Disruptor<ByteBuf>(new EventFactory() {
			public Object newInstance() {
				return Unpooled.buffer(30);
			}
		}, 1024 * ringBufferSize, m_executor);
		disruptor.handleEventsWith(this);
		disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		m_ringBuffer = disruptor.getRingBuffer();

		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		ssdb = SSDBs.pool(ssdbIp, ssdbPort, 10000, config);
		groupNotifyService = new GroupNotifyService(mqttconsoleUrl);
		groupNotifyService.init();

		customEvtMqProducer = new DefaultMQProducer(String.format("MQTT_CUSTOM_EVENT_MQ_PG%d",brokergid));
		
		m_decoderMap.put(MessageType.PUBLISH, new PublishProcessor(this));
		initHttpClient();
	}

	private void initHttpClient() {
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
		running = true;
		try {
			customEvtMqProducer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
		// publish process
		new Thread() {
			public void run() {
				do {
					try {
						Response resp = ssdb.qpop_front("MQTT-UP-TG-0", 10000);
						List<byte[]> datas = resp.datas;
						if (resp.ok() && datas.size()>0) {
							for (byte[] data : datas)
								disruptorProcess(data);
						} else {
							try {
								Thread.currentThread().sleep(10);
							} catch (InterruptedException e) {
								LOG.info(null, e);
							}
						}
					} catch (Exception e) {
						LOG.error(null, e);
						try {
							Thread.currentThread().sleep(5000);
						} catch (InterruptedException e1) {
						}
					}
				} while (running);
			}
		}.start();
		// 配置同步
		new Thread() {
			public void run() {
				HttpGet getRequest = new HttpGet(mqttconsoleUrl);
				boolean initFlg = true;
				do {
					try {
						try {
							HttpResponse response = httpClient.execute(getRequest);
							String content = EntityUtils.toString(response.getEntity());
							content = StringUtils.replace(content, "\n", "");
							if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(content)) {
								String[] array = StringUtils.split(content, "&");
								for (String strkv : array) {
									String[] kv = StringUtils.split(strkv, "=");
									if (StringUtils.startsWith(kv[0], "G/LIMIT/"))
										continue;
									Response resp = ssdb.hset("mqtt.configure", kv[0], kv[1]);
									if (!resp.ok()) {
										LOG.info("name=" + resp.stat);
									}
								}
							} else {
								initFlg = true;
							}
						} catch (IOException e) {
							LOG.error("回调异常{}", e);
						} finally {
							getRequest.releaseConnection();
						}
						if (initFlg) {
							Response resp = ssdb.hgetall("mqtt.configure");
							if (!resp.ok()) {
								LOG.info("stat:" + resp.stat);
								continue;
							}
							Map<String, String> datas = resp.mapString();
							List<NameValuePair> params = new ArrayList<NameValuePair>();
							for (Map.Entry<String, String> entry : datas.entrySet()) {
								params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
							}
							HttpPost postRequest = new HttpPost(mqttconsoleUrl);

							try {
								postRequest.setEntity(new UrlEncodedFormEntity(params));
								HttpResponse response = httpClient.execute(postRequest);
								String content = EntityUtils.toString(response.getEntity());
								if (response.getStatusLine().getStatusCode() == 200 && StringUtils.isNotBlank(content)) {
									initFlg = false;
								}
							} catch (IOException e) {
								LOG.error("回调异常{}", e);
							} finally {
								postRequest.releaseConnection();
							}
						}

						try {
							Thread.currentThread().sleep(5000);
						} catch (InterruptedException e) {
							LOG.info(null, e);
						}
					} catch (Exception e) {
						LOG.error(null, e);
						try {
							Thread.currentThread().sleep(5000);
						} catch (InterruptedException e1) {
						}
					}
				} while (running);
			}
		}.start();
		groupNotifyService.start();
	}

	public void destroy() {
		running = false;
		m_executor.shutdown();
		groupNotifyService.stop();
	}

	public void onEvent(ByteBuf in, long sequence, boolean endOfBatch) throws Exception {
		in.resetReaderIndex();
		int startPos = in.readerIndex();

		byte h1 = in.readByte();
		byte messageType = (byte) ((h1 & 0x00F0) >> 4);

		MqttProcessor processor = m_decoderMap.get(messageType);
		processor.process(in, startPos, h1);
	}

	/**
	 * byte数组中取int数值，本方法适用于(低位在前，高位在后)的顺序，和和intToBytes（）配套使用
	 * 
	 * @param src
	 *            byte数组
	 * @param offset
	 *            从数组的第offset位开始
	 * @return int数值
	 */
	public static int bytesToInt(byte[] src, int offset) {
		int value;
		value = (int) ((src[offset] & 0xFF) | ((src[offset + 1] & 0xFF) << 8) | ((src[offset + 2] & 0xFF) << 16) | ((src[offset + 3] & 0xFF) << 24));
		return value;
	}

	/**
	 * byte数组中取int数值，本方法适用于(低位在后，高位在前)的顺序。和intToBytes2（）配套使用
	 */
	public static int bytesToInt2(byte[] src, int offset) {
		int value;
		value = (int) (((src[offset] & 0xFF) << 24) | ((src[offset + 1] & 0xFF) << 16) | ((src[offset + 2] & 0xFF) << 8) | (src[offset + 3] & 0xFF));
		return value;
	}

	private void disruptorProcess(byte[] msgEvent) {
		LOG.debug("disruptorPublish publishing event on output {}", msgEvent);
		long sequence = m_ringBuffer.next();
		try {
			ByteBuf event = (ByteBuf) m_ringBuffer.get(sequence);
			event.clear();
			event.writeBytes(msgEvent);
		} finally {
			m_ringBuffer.publish(sequence);
		}
	}

	public static void main(String[] args) {
		Properties configProps = new Properties();
		MQTTUPConsumer self = new MQTTUPConsumer(configProps);
		self.init();
		self.start();
		// self.destroy();
	}

	public SSDB getSsdb() {
		return ssdb;
	}

	public GroupNotifyService getGroupNotifyService() {
		return groupNotifyService;
	}

	public Properties getConfigProps() {
		return configProps;
	}

	public Integer getBrokergid() {
		return brokergid;
	}

	public DefaultMQProducer getCustomEvtMqProducer() {
		return customEvtMqProducer;
	}

}

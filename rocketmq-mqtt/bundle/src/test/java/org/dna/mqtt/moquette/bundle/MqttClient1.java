package org.dna.mqtt.moquette.bundle;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.Before;
import org.junit.Test;

public class MqttClient1 {
	CallbackConnection callbackConnection = null;
	boolean isconnect=false;
	@Test
	public void test(){
		int i=0;
		do{
			if(isconnect){
				// 订阅主题
				Topic[] topics = { new Topic("foo", QoS.AT_LEAST_ONCE) };
				callbackConnection.subscribe(topics, new Callback<byte[]>() {
					// 订阅主题成功
					public void onSuccess(byte[] qoses) {
						System.out.println("========订阅成功=======");
					}

					// 订阅主题失败
					public void onFailure(Throwable value) {
						System.out.println("========订阅失败=======");
						callbackConnection.disconnect(null);
					}
				});

				// 发布消息
				callbackConnection.publish("foo", ("Hello ").getBytes(), QoS.AT_LEAST_ONCE, true, new Callback<Void>() {
					public void onSuccess(Void v) {
						System.out.println("===========消息发布成功============");
					}

					public void onFailure(Throwable value) {
						System.out.println("========消息发布失败=======");
						callbackConnection.disconnect(null);
					}
				});
				
				UTF8Buffer[] utf8buffers = {UTF8Buffer.utf8("foo")};
				callbackConnection.unsubscribe(utf8buffers, new Callback<Void>() {
					// 取消订阅主题成功
					public void onSuccess(Void v) {
						System.out.println("========UN订阅成功=======");
					}

					// 取消订阅主题失败
					public void onFailure(Throwable value) {
						System.out.println("========UN订阅失败=======");
						callbackConnection.disconnect(null);
					}
				});
				
				i++;
				
			}
			try {Thread.sleep(100);} catch (InterruptedException e) {}
		}while(i<5);
		
		callbackConnection.disconnect(new Callback<Void>() {
			// 取消订阅主题成功
			public void onSuccess(Void v) {
				System.out.println("========disconnect成功=======");
			}

			// 取消订阅主题失败
			public void onFailure(Throwable value) {
				System.out.println("========disconnect失败=======");
			}
		});
	}
	
	@Before
	public void init() throws Exception{
		MQTT mqtt = new MQTT();
		mqtt.setHost("tcp://127.0.0.1:1883");
		mqtt.setClientId("clientId-1");
		mqtt.setWillQos(QoS.AT_MOST_ONCE);
		mqtt.setDispatchQueue(Dispatch.createQueue("foo"));
		mqtt.setTracer(new Tracer() {
			@Override
			public void onReceive(MQTTFrame frame) {
				System.out.println("recv: " + frame);
			}

			@Override
			public void onSend(MQTTFrame frame) {
				System.out.println("send: " + frame);
			}

			@Override
			public void debug(String message, Object... args) {
				System.out.println(String.format("debug: " + message, args));
			}
		});
		callbackConnection = mqtt.callbackConnection();

		// 连接监听
		callbackConnection.listener(new Listener() {
			public void onFailure(Throwable value) {
				System.out.println("===========connect failure===========");
				callbackConnection.disconnect(null);
			}

			public void onDisconnected() {
				System.out.println("====mqtt disconnected=====");

			}

			public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
				System.out.println("=============receive msg================" + new String(body.toByteArray()));
				ack.run();
			}

			public void onConnected() {
				System.out.println("====mqtt connected=====");
			}
		});

		// 连接
		callbackConnection.connect(new Callback<Void>() {

			// 连接失败
			public void onFailure(Throwable value) {
				System.out.println("============连接失败：" + value.getLocalizedMessage() + "============");
			}

			// 连接成功
			public void onSuccess(Void v) {
				isconnect=true;
			}
		});
		

	}

}

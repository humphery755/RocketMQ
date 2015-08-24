package org.dna.mqtt.moquette.bundle;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class MqttWSClient {
	public static void main(String[] args) throws URISyntaxException, InterruptedException {
		final MqttWSClient self = new MqttWSClient();
        String destUri = "ws://192.168.92.13:6080/mqtt";
        destUri = "ws://192.168.88.213:6080/mqtt";
        if (args.length > 0) {
            destUri = args[0];
        }
        WebSocketClient client = new WebSocketClient();
        client.setMaxBinaryMessageBufferSize(1024*1024);
        MqttSocket socket = new MqttSocket();
        try {
            client.start();
            URI echoUri = new URI(destUri);
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(socket, echoUri, request);
            
            System.out.printf("Connecting to : %s%n", echoUri);
            socket.awaitClose(500, TimeUnit.SECONDS);
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }	
	
}



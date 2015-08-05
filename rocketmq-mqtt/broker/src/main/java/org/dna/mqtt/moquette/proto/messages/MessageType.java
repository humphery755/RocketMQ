package org.dna.mqtt.moquette.proto.messages;


public abstract class MessageType {
	public final static byte RESERVED_0 = 0;// Reserved 
	public final static byte CONNECT = 1; // Client request to connect to Server
	public final static byte CONNACK = 2; // Connect Acknowledgment
	public final static byte PUBLISH = 3; // Publish message
	public final static byte PUBACK = 4; // Publish Acknowledgment
	public final static byte PUBREC = 5; //Publish Received (assured delivery part 1)
	public final static byte PUBREL = 6; // Publish Release (assured delivery part 2)
	public final static byte PUBCOMP = 7; //Publish Complete (assured delivery part 3)
	public final static byte SUBSCRIBE = 8; //Client Subscribe request
	public final static byte SUBACK = 9; // Subscribe Acknowledgment
	public final static byte UNSUBSCRIBE = 10; //Client Unsubscribe request
	public final static byte UNSUBACK = 11; // Unsubscribe Acknowledgment
	public final static byte PINGREQ = 12; //PING Request
	public final static byte PINGRESP = 13; //PING Response
	public final static byte DISCONNECT = 14; //Client is Disconnecting
	public final static byte RESERVED_15 = 15; // Reserved
    
    public final static byte QOS_MOST_ONE=0, QOS_LEAST_ONE=1, QOS_EXACTLY_ONCE=2, QOS_RESERVED=3;
        
    public static String formatQoS(byte qos) {
    	switch(qos){
    	case QOS_MOST_ONE:
    		return String.format("%d - MOST_ONE", qos);
    	case QOS_LEAST_ONE:
    		return String.format("%d - LEAST_ONE", qos);
    	case QOS_EXACTLY_ONCE: 
    		return String.format("%d - EXACTLY_ONCE", qos);
    	case QOS_RESERVED:
    		return String.format("%d - RESERVED", qos);
    	}
        return String.format("%d - UNKNOWN", qos);
    }
}

package org.dna.mqtt.moquette.messaging.spi.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;
import org.dna.mqtt.moquette.proto.messages.MessageType;

public class TopicRuleTest {
	private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<String/* clientId */, Subscription>> subscriptionMap = new ConcurrentHashMap();
	
	public static void main(String[] args) {
		TopicRuleTest test=new TopicRuleTest();
		int index=0;
		Set<String> cidSet = new HashSet();
		ConcurrentHashMap m=new ConcurrentHashMap();
		String topic = "G/LIMIT/a/b";
		String cId="P/cid-"+(index++);
		m.put(cId, new Subscription(cId,topic,MessageType.QOS_MOST_ONE,true,-1));
		test.subscriptionMap.put(topic, m);
		
		topic = "G/#";
		m=new ConcurrentHashMap();
		cId="P/cid-"+(index++);
		m.put(cId, new Subscription(cId,topic,MessageType.QOS_MOST_ONE,true,-1));
		test.subscriptionMap.put(topic, m);
		
		topic = "G/a/+/c";
		m=new ConcurrentHashMap();
		cId="P/cid-"+(index++);
		m.put(cId, new Subscription(cId,topic,MessageType.QOS_MOST_ONE,true,-1));
		test.subscriptionMap.put(topic, m);
		
		topic = "G/a/+";
		m=new ConcurrentHashMap();
		cId="P/cid-"+(index++);
		m.put(cId, new Subscription(cId,topic,MessageType.QOS_MOST_ONE,true,-1));
		test.subscriptionMap.put(topic, m);
		//******************************************************************************************/
		topic = "G/a/b/c";
		cidSet =test.findClientsByTopic(topic,cidSet );
		System.out.println(topic+":"+cidSet.toString());
		
		cidSet.clear();
		topic = "G/a/xxx/c";
		cidSet = test.findClientsByTopic(topic,cidSet );
		System.out.println(topic+":"+cidSet.toString());
		
		cidSet.clear();
		topic = "G/a/yc";
		cidSet =test.findClientsByTopic(topic,cidSet );
		System.out.println(topic+":"+cidSet.toString());
		
		cidSet.clear();
		topic = "G/a/bb/c";
		cidSet =test.findClientsByTopic(topic,cidSet );
		System.out.println(topic+":"+cidSet.toString());
		
		cidSet.clear();
		topic = "G/B";
		cidSet =test.findClientsByTopic(topic,cidSet );
		System.out.println(topic+":"+cidSet.toString());
	}
	
	/**
	关于Topic通配符
	/：用来表示层次，比如a/b，a/b/c。
	#：表示匹配>=0个层次，比如a/#就匹配a/，a/b，a/b/c。
	单独的一个#表示匹配所有。
	不允许 a#和a/#/c。
	+：表示匹配一个层次，例如a/+匹配a/b，a/c，不匹配a/b/c。
	单独的一个+是允许的，a+不允许，a/+/b允许
	*/
	private Set<String> findClientsByTopic(String topic,Set<String> set){
		char c,oc;
		int topLen = topic.length();
		boolean flg;
		for (Map.Entry<String/* topic */, ConcurrentHashMap<String/* clientId */, Subscription>> entry : subscriptionMap.entrySet()) {
			int seachLen = entry.getKey().length();
			if(topLen<seachLen)continue;
			flg = true;
			int j=0;
			for(int i=0;i<seachLen && flg;i++){
				c = entry.getKey().charAt(i);
				switch(c){
				case '+':
					do{
						oc = topic.charAt(j++);
					}while(oc!='/'&&j<topLen);
					if(j<topLen){
						if(i>=seachLen-1){
							flg=false;
							break;
						}
						j--;
					}else{
						set.addAll(entry.getValue().keySet());
						flg=false;
					}
					break;
				case '#':
					set.addAll(entry.getValue().keySet());
					flg=false;
					break;
				case '/':
					oc = topic.charAt(j++);
					if(c!=oc){
						if(i-j==2){
							set.addAll(entry.getValue().keySet());
							flg=false;
							break;
						}
						flg=false;
					}
					break;
				default:
					oc = topic.charAt(j++);
					if(c!=oc){
						flg=false;
						break;
					}
					break;
				}
			}
			if(flg && topLen==j)
				set.addAll(entry.getValue().keySet());
		}
		return set;
	}
}

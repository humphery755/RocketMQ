package org.dna.mqtt.moquette.bundle;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.dna.mqtt.moquette.server.Constants;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class BuilderTopic {
	@Test
	public void test() throws IOException {
		String topics = IOUtils.toString(BuilderTopic.class.getResource("./template-topics.json"));
		JSONObject root = JSON.parseObject(topics);
		JSONObject topicConfigTable = root.getJSONObject("topicConfigTable");
		String topicGroup;

		topicGroup = String.format(Constants.TOPIC_DOWNG_PATTERN,0);
		JSONObject tmp = new JSONObject();
		tmp.put("order", false);
		tmp.put("perm", 6);
		tmp.put("readQueueNums", 1);
		tmp.put("topicFilterType", "SINGLE_TAG");
		tmp.put("topicName", topicGroup);
		tmp.put("topicSysFlag", 0);
		tmp.put("writeQueueNums", 1);
		topicConfigTable.put(topicGroup, tmp);

		topicGroup = String.format(Constants.TOPIC_UPG_PATTERN,0);
		tmp = new JSONObject();
		tmp.put("order", false);
		tmp.put("perm", 6);
		tmp.put("readQueueNums", 1);
		tmp.put("topicFilterType", "SINGLE_TAG");
		tmp.put("topicName", topicGroup);
		tmp.put("topicSysFlag", 0);
		tmp.put("writeQueueNums", 1);
		topicConfigTable.put(topicGroup, tmp);

		topicGroup = String.format(Constants.TOPIC_SYS_PATTERN,0);
		tmp = new JSONObject();
		tmp.put("order", false);
		tmp.put("perm", 6);
		tmp.put("readQueueNums", 1);
		tmp.put("topicFilterType", "SINGLE_TAG");
		tmp.put("topicName", topicGroup);
		tmp.put("topicSysFlag", 0);
		tmp.put("writeQueueNums", 1);
		topicConfigTable.put(topicGroup, tmp);


		URL url = BuilderTopic.class.getResource(".");

		OutputStream os = new FileOutputStream(url.getFile() + "/topics.json");
		IOUtils.write(root.toJSONString(), os);
		os.close();
		System.out.println(url.getFile() + "/topics.json");

		String subscriptionGroup = IOUtils.toString(BuilderTopic.class.getResource("./template-subscriptionGroup.json"));

	}
}

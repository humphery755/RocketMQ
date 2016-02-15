package com.alibaba.rocketmq.action;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.service.MessageService;
import com.alibaba.rocketmq.service.OffsetService;


/**
 * 
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-19
 */
@Controller
@RequestMapping("/offset")
public class OffsetAction extends AbstractAction {

    @Autowired
    OffsetService offsetService;
    
    @Autowired
    MessageService messageService;

    @Override
    protected String getFlag() {
        return "offset_flag";
    }


    @RequestMapping(value = "/resetOffsetByTime.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String resetOffsetByTime(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String group, @RequestParam(required = false) String topic,
            @RequestParam(required = false) String timestamp, @RequestParam(required = false) String force) {
        Collection<Option> options = offsetService.getOptionsForResetOffsetByTime();
        putPublicAttribute(map, "resetOffsetByTime", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                Table table = offsetService.resetOffsetByTime(group, topic, timestamp, force);
                putTable(map, table);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }
    
    @RequestMapping(value = "/viewMessageByPhyOffset.do", method = { RequestMethod.GET, RequestMethod.POST})
    public String viewMessageByPhyOffset(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String addr, @RequestParam(required = false) Long offset) {
        Options options=new Options();
        Option opt = new Option("a", "addr", true, "set the broker address");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "set the offset");
        opt.setRequired(true);
        options.addOption(opt);
        putPublicAttribute(map, "viewMessageByPhyOffset", options.getOptions(), request);
        try {
        	 if (request.getMethod().equals(GET)) {

             }
             else if (request.getMethod().equals(POST)) {
            	checkOptions(options.getOptions());
            	ByteBuffer input=ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                // 地址
            	int index=addr.indexOf(":");
            	String ip=addr.substring(0,index);
               int port = Integer.valueOf(addr.substring(index+1));
               InetSocketAddress address = new InetSocketAddress(ip, port);
                input.put(address.getAddress().getAddress());
                input.putInt(port);
                input.putLong(offset);
            	String msgId=UtilAll.bytes2string(input.array());
                Table table = messageService.queryMsgById(msgId);
                putTable(map, table);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @Override
    protected String getName() {
        return "Offset";
    }
}

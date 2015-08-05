package org.dna.mqtt.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class ObjectUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ObjectUtils.class);
	
	public static void main(String[] args){
		PublishMessage pm=new PublishMessage();
		pm.setPayload(ByteBuffer.allocateDirect(1024));
		byte a[] = ObjectUtils.serialize(pm);
		ObjectUtils.deserialize(a, PublishMessage.class);
		return;
	}

	static KryoFactory factory = new KryoFactory() {
		  public Kryo create () {
		    Kryo kryo = new Kryo();
		    kryo.setRegistrationRequired(false);
	        kryo.setMaxDepth(20);
		    // configure kryo instance, customize settings java.nio.DirectByteBuffer
		    return kryo;
		  }
		};
		
	private final static KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
	
	public static <T>T deserialize(byte[] args,Class<T> cls){
		ByteArrayInputStream is = new ByteArrayInputStream(args);
		Input input = new Input(is);
		Kryo kryo = pool.borrow();
		try {
			return (T)kryo.readClassAndObject(input);
		} catch (Exception e) {
			LOG.error("", e);
		}finally {
			pool.release(kryo);
			if (null != is) {
                try {
                	is.close();
                	is = null;
                } catch (IOException e) {
                }
            }
            if (null != input) {
                input.close();
                input = null;
            }
        }
		return null;
	}
	public static byte[] serialize(Object obj){
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Output output = new Output(out);
		Kryo kryo = pool.borrow();
		try {
			kryo.writeClassAndObject(output, obj);
            return output.toBytes();
		} catch (Exception e) {
			LOG.error("",e);
		}finally {
			pool.release(kryo);
            if (null != out) {
                try {
                    out.close();
                    out = null;
                } catch (IOException e) {
                }
            }
            if (null != output) {
                output.close();
                output = null;
            }
        }
		return null;
	}
}

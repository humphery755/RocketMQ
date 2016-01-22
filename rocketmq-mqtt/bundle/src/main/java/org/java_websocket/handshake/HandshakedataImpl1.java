package org.java_websocket.handshake;

import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

public class HandshakedataImpl1 implements HandshakeBuilder {
	private byte[] content;
	private TreeMap<String,String> map;

	public HandshakedataImpl1() {
		map = new TreeMap<String,String>( String.CASE_INSENSITIVE_ORDER );
	}

	/*public HandshakedataImpl1( Handshakedata h ) {
		httpstatusmessage = h.getHttpStatusMessage();
		resourcedescriptor = h.getResourceDescriptor();
		content = h.getContent();
		map = new LinkedHashMap<String,String>();
		Iterator<String> it = h.iterateHttpFields();
		while ( it.hasNext() ) {
			String key = (String) it.next();
			map.put( key, h.getFieldValue( key ) );
		}
	}*/

	
	public Iterator<String> iterateHttpFields() {
		return Collections.unmodifiableSet( map.keySet() ).iterator();// Safety first
	}

	
	public String getFieldValue( String name ) {
		String s = map.get( name );
		if ( s == null ) {
			return "";
		}
		return s;
	}

	
	public byte[] getContent() {
		return content;
	}

	
	public void setContent( byte[] content ) {
		this.content = content;
	}

	
	public void put( String name, String value ) {
		map.put( name, value );
	}

	
	public boolean hasFieldValue( String name ) {
		return map.containsKey( name );
	}
}

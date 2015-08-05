package org.dna.mqtt.moquette.bundle;

import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.events.OutputMessagingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorTest {
	AtomicInteger at_count = new AtomicInteger(0);
	private ExecutorService m_executor;
	private RingBuffer<ValueEvent> m_ringBuffer;
	TestEvent event = new TestEvent();

	@Before
	public void setUp() throws Exception {
		m_executor = Executors.newFixedThreadPool(1);
		Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 4, m_executor);
		disruptor.handleEventsWith(event);
		disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		m_ringBuffer = disruptor.getRingBuffer();
	}

	@After
	public void tearDown() throws Exception {
		synchronized (DisruptorTest.class) {
			try {
				DisruptorTest.class.wait();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		fail("Not yet implemented");
	}

	private void disruptorPublish(OutputMessagingEvent msgEvent) {
		long sequence = m_ringBuffer.next();
		try {
			ValueEvent event = m_ringBuffer.get(sequence);
			event.setEvent(msgEvent);
		} finally {
			m_ringBuffer.publish(sequence);
		}
		at_count.incrementAndGet();
	}

	@Test
	public void test() {
		ExecutorService set_executor = Executors.newFixedThreadPool(100);
		set_executor.execute(new Thread() {
			public void run() {
				do{
					OutputMessagingEvent e = new OutputMessagingEvent(null, null);
					disruptorPublish(e);
				}while(true);
			}
		});

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(new Thread() {
			public void run() {
				System.out.println(at_count);
			}
		}, 10000, 10000, TimeUnit.MILLISECONDS);

	}

	class TestEvent implements EventHandler<ValueEvent> {

		public void onEvent(ValueEvent event, long sequence, boolean endOfBatch) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("onEvent");
			Thread.currentThread().sleep(10000);
		}

	}
}

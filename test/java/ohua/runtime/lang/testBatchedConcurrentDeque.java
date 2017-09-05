/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.ConcurrentArcQueue;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import org.junit.Test;
import org.junit.Assert;

import java.util.Deque;
import java.util.stream.IntStream;

/**
 * Created by sertel on 12/23/16.
 */
public class testBatchedConcurrentDeque {

  @Test
  public void testSingleThreaded() throws Throwable {
    ConcurrentArcQueue.BatchedConcurrentLinkedDeque.BATCH_SIZE = 5;
    Deque<Object> deque = new ConcurrentArcQueue.BatchedConcurrentLinkedDeque<>();

    // poll on empty
    Assert.assertNull(deque.peek());
    Assert.assertNull(deque.poll());
    Assert.assertEquals(0, deque.size());
    Assert.assertTrue(deque.isEmpty());

    // adding 4 items in
    IntStream.range(1,5).forEachOrdered(deque::add);

    // poll on queue whose batch has not arrived yet
    Assert.assertNull(deque.peek());
    Assert.assertNull(deque.poll());
    Assert.assertEquals(4, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // adding the missing item
    deque.add(5);

    // peeking to check that value arrived
    Assert.assertEquals(1, deque.peek());
    Assert.assertEquals(5, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // polling some but not all values
    Assert.assertEquals(1, deque.poll());
    Assert.assertEquals(2, deque.poll());
    Assert.assertEquals(3, deque.poll());
    Assert.assertEquals(2, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // adding some new values
    IntStream.range(6,12).forEachOrdered(deque::add);
    Assert.assertEquals(4, deque.peek());
    Assert.assertEquals(8, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // polling the last two of the first batch and next from the second batch which must already have been transferred
    Assert.assertEquals(4, deque.poll());
    Assert.assertEquals(5, deque.poll());
    Assert.assertEquals(6, deque.poll());
    Assert.assertEquals(7, deque.poll());
    Assert.assertEquals(4, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // I can poll 3 more but the last ones must be flushed
    Assert.assertEquals(8, deque.poll());
    Assert.assertEquals(9, deque.poll());
    Assert.assertEquals(10, deque.poll());
    Assert.assertEquals(1, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // although the batch is incomplete (size = 2), this special packet should flush it to the receiver side
    EndOfStreamPacket eos = PacketFactory.createEndSignalPacket(0);
    deque.add(eos);
    Assert.assertEquals(2, deque.size());
    Assert.assertFalse(deque.isEmpty());

    // finally I can deque my last entry
    Assert.assertEquals(11, deque.poll());
    Assert.assertEquals(eos, deque.poll());
    Assert.assertEquals(0, deque.size());
    Assert.assertTrue(deque.isEmpty());
  }
}

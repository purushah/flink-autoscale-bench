package dev.jevscaler.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** The restore delay stands in for loading a large state; with the new argument it spares the first start. */
class RestoreDelayTest {

    @Test
    void theFirstStartWaitsByDefault() {
        assertEquals(60000, BenchJob.delayFor(false, true, 60000));
    }

    @Test
    void theFirstStartDoesNotWaitWhenToldSo() {
        assertEquals(0, BenchJob.delayFor(false, false, 60000));
    }

    @Test
    void aRestoreAlwaysWaits() {
        assertEquals(60000, BenchJob.delayFor(true, false, 60000));
        assertEquals(60000, BenchJob.delayFor(true, true, 60000));
    }

    @Test
    void noDelayIsNoDelay() {
        assertEquals(0, BenchJob.delayFor(true, true, 0));
    }
}

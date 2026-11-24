package dev.jevscaler.job;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.Deflater;
import org.junit.jupiter.api.Test;

/** The ballast is real state of a key: entries of a fixed size, written once; every record reads one of the first few. */
class BallastTest {

    @Test
    void noBytesPerKeyMeansNoEntries() {
        assertEquals(0, BenchJob.Ballast.entries(0, 4096));
        assertEquals(0, BenchJob.Ballast.entries(-1, 4096));
    }

    @Test
    void theEntriesHoldAtLeastTheBytesAsked() {
        assertEquals(1024, BenchJob.Ballast.entries(4L << 20, 4096));
        assertEquals(2, BenchJob.Ballast.entries(5000, 4096));
        assertEquals(1, BenchJob.Ballast.entries(1, 4096));
    }

    @Test
    void anEntryHasTheSizeAsked() {
        assertEquals(4096, BenchJob.Ballast.entry("k7", 3, 4096).length);
    }

    @Test
    void anEntryIsTheSameForTheSameKeyAndIndex() {
        assertArrayEquals(BenchJob.Ballast.entry("k7", 3, 4096), BenchJob.Ballast.entry("k7", 3, 4096));
    }

    @Test
    void entriesOfOtherKeysAndIndexesDiffer() {
        byte[] one = BenchJob.Ballast.entry("k7", 3, 4096);
        assertFalse(Arrays.equals(one, BenchJob.Ballast.entry("k7", 4, 4096)));
        assertFalse(Arrays.equals(one, BenchJob.Ballast.entry("k8", 3, 4096)));
    }

    @Test
    void anEntryDoesNotCompress() {
        byte[] entry = BenchJob.Ballast.entry("k7", 3, 4096);
        var deflater = new Deflater(Deflater.BEST_COMPRESSION);
        deflater.setInput(entry);
        deflater.finish();
        int packed = deflater.deflate(new byte[8192]);
        deflater.end();
        assertTrue(packed >= 4000, "an entry of 4096 bytes packed to " + packed);
    }

    @Test
    void theRecordsOfAKeyReadItsFirstEntriesInTurn() {
        // A key with 512 entries: its records read the first eight, which a cache can hold.
        var read = new HashSet<Integer>();
        for (long record = 1; record <= 100; record++) {
            read.add(BenchJob.Ballast.slot(record, 512));
        }
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7)), read);
        assertEquals(BenchJob.Ballast.slot(1, 512), BenchJob.Ballast.slot(9, 512));
    }

    @Test
    void aKeyWithFewEntriesReadsAllOfThem() {
        var read = new HashSet<Integer>();
        for (long record = 1; record <= 100; record++) {
            read.add(BenchJob.Ballast.slot(record, 3));
        }
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)), read);
    }

    @Test
    void aKeyWritesItsEntriesOverItsFirstRecords() {
        // 512 entries, 16 with each record: the first 32 records of the key write them, in order.
        assertArrayEquals(new int[] {0, 16}, BenchJob.Ballast.fillRange(1, 512, 16));
        assertArrayEquals(new int[] {16, 32}, BenchJob.Ballast.fillRange(2, 512, 16));
        assertArrayEquals(new int[] {496, 512}, BenchJob.Ballast.fillRange(32, 512, 16));
    }

    @Test
    void aKeyWhoseEntriesAreWrittenWritesNoMore() {
        int[] range = BenchJob.Ballast.fillRange(33, 512, 16);
        assertTrue(range[0] >= range[1]);
        range = BenchJob.Ballast.fillRange(1_000_000, 512, 16);
        assertTrue(range[0] >= range[1]);
    }

    @Test
    void theLastStepWritesWhatIsLeft() {
        assertArrayEquals(new int[] {0, 16}, BenchJob.Ballast.fillRange(1, 20, 16));
        assertArrayEquals(new int[] {16, 20}, BenchJob.Ballast.fillRange(2, 20, 16));
    }

    @Test
    void theFirstRecordWritesAtLeastTheEntriesThatRecordsRead() {
        int[] range = BenchJob.Ballast.fillRange(1, 512, 2);
        assertEquals(0, range[0]);
        assertTrue(range[1] >= BenchJob.Ballast.HOT);
    }

    @Test
    void aKeyWithoutEntriesWritesNothing() {
        int[] range = BenchJob.Ballast.fillRange(1, 0, 16);
        assertTrue(range[0] >= range[1]);
    }
}

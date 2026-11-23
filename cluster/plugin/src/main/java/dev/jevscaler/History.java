package dev.jevscaler;

import dev.jevscaler.JevRuntime.Sample;
import dev.jevscaler.Model.ScalingEvent;
import dev.jevscaler.Model.VertexSnapshot;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * What the job's own recent measurements say beyond the current cycle: the highest demand of a
 * window, the source's rate over the last half hour, and what earlier changes cost.
 *
 * <p>There is one sample per autoscaler cycle in which the metric window was full, oldest first. So
 * after every change the samples have a gap: the restart, the stabilization interval and one
 * metric window.
 */
final class History {

    /** Ages, in seconds, at which {@link #demandHistory} reports the sources' rate. */
    static final int[] AGES = {60, 120, 180, 300, 420, 600, 900, 1200, 1500, 1800};

    /** A sample counts for an age when it is at most this many seconds away from it. */
    static final int AGE_TOLERANCE_SECONDS = 20;

    /** How long after a change its backlog peak is looked for. */
    static final int AFTERMATH_SECONDS = 300;

    private History() {}

    static Set<String> sources(List<VertexSnapshot> snapshots) {
        return snapshots.stream().filter(v -> "source".equals(v.role)).map(v -> v.id).collect(Collectors.toSet());
    }

    /**
     * The sum over the given vertices of one value of a sample (0: demand, 1: backlog). NaN when
     * the sample has that value for none of them.
     */
    static double sum(Sample s, Set<String> ids, int index) {
        double total = 0;
        boolean any = false;
        for (var id : ids) {
            var values = s.perVertex.get(id);
            if (values != null && !Double.isNaN(values[index])) {
                total += values[index];
                any = true;
            }
        }
        return any ? total : Double.NaN;
    }

    /**
     * Sets every vertex's {@link VertexSnapshot#peakRate} to its highest demand within the window,
     * and its {@link VertexSnapshot#priorPeakRate} to the highest demand of the window's samples
     * that are at least {@code excludeSeconds} old: the level the job carried before a rise that
     * is under way now.
     */
    static void applyPeaks(
            Collection<Sample> samples, List<VertexSnapshot> snapshots, Instant now, int windowSeconds, int excludeSeconds) {
        for (var v : snapshots) {
            double peak = Double.NaN;
            double prior = Double.NaN;
            for (var s : samples) {
                long age = Duration.between(s.time, now).toSeconds();
                if (age > windowSeconds) {
                    continue;
                }
                var values = s.perVertex.get(v.id);
                if (values == null || Double.isNaN(values[0])) {
                    continue;
                }
                // Written so that the first value replaces the NaN the peaks start with.
                if (!(values[0] <= peak)) {
                    peak = values[0];
                }
                if (age >= excludeSeconds && !(values[0] <= prior)) {
                    prior = values[0];
                }
            }
            v.peakRate = peak;
            v.priorPeakRate = prior;
        }
    }

    /**
     * The sources' rate at {@link #AGES}, newest first, as {seconds ago, records per second}. An
     * age without a sample close to it (the job was restarting, or is younger) is left out.
     */
    static List<double[]> demandHistory(Collection<Sample> samples, List<VertexSnapshot> snapshots, Instant now) {
        var ids = sources(snapshots);
        var out = new ArrayList<double[]>();
        for (int age : AGES) {
            Sample best = null;
            long bestDistance = Long.MAX_VALUE;
            for (var s : samples) {
                long distance = Math.abs(Duration.between(s.time, now).toSeconds() - age);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    best = s;
                }
            }
            if (best == null || bestDistance > AGE_TOLERANCE_SECONDS) {
                continue;
            }
            double rate = sum(best, ids, 0);
            if (!Double.isNaN(rate)) {
                out.add(new double[] {age, rate});
            }
        }
        return out;
    }

    /**
     * Fills in what each past change cost, for the changes the samples cover on both sides: how
     * long the job gave no measurements, the sources' backlog just before, its peak within {@link
     * #AFTERMATH_SECONDS} after, and when it was back at the level before the change (or below two
     * seconds of input).
     */
    static void applyOutcomes(Collection<Sample> samples, List<VertexSnapshot> snapshots, List<ScalingEvent> history) {
        var ids = sources(snapshots);
        for (var e : history) {
            Sample before = null;
            Sample firstAfter = null;
            double peak = Double.NaN;
            for (var s : samples) {
                if (!s.time.isAfter(e.time)) {
                    before = s;
                    continue;
                }
                if (firstAfter == null) {
                    firstAfter = s;
                }
                if (Duration.between(e.time, s.time).toSeconds() <= AFTERMATH_SECONDS) {
                    double backlog = sum(s, ids, 1);
                    if (!Double.isNaN(backlog) && !(backlog <= peak)) {
                        peak = backlog;
                    }
                }
            }
            if (before == null || firstAfter == null) {
                continue;
            }
            e.measuredAgainAfterSeconds = Duration.between(e.time, firstAfter.time).toSeconds();
            e.backlogBefore = sum(before, ids, 1);
            e.backlogPeakAfter = peak;
            for (var s : samples) {
                if (!s.time.isAfter(e.time)) {
                    continue;
                }
                double backlog = sum(s, ids, 1);
                double rate = sum(s, ids, 0);
                double level =
                        Math.max(
                                Double.isNaN(e.backlogBefore) ? 0 : e.backlogBefore,
                                Double.isNaN(rate) ? 0 : 2 * rate);
                if (!Double.isNaN(backlog) && backlog <= level) {
                    e.backlogClearedAfterSeconds = Duration.between(e.time, s.time).toSeconds();
                    break;
                }
            }
        }
    }
}

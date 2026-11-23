"""The step-aware demand on the cluster (Tests A and C) as LaTeX macros, from the monitoring extracts.
   python3 steptex.py <campaign dir> <scenario> <tex file> <macro prefix>
   e.g. python3 steptex.py results-cluster/stepfix surge_long paper/gen/numbers_clsteplong.tex ClStepLong
Reads <campaign dir>/datadog.json (seconds over the limit and slots per job and run, stored by ddadd.py)
and writes, for the shipped band and the band at the target: the mean seconds over the limit without
and with the option, the paired difference with its t interval, in how many runs it is lower, the share
of the time that is gone, the exact sign-flip p, and the same difference for the slots. The counts from
the operator's log (first orders, seconds until the load is covered) are printed by stepscore.py."""
import json, os, sys
import sigsum

PAIRS = (("Stock", "builtin+lagfix", "builtin+lagfix+b"), ("Target", "builtin+max70+lagfix", "builtin+max70+lagfix+b"))


def main():
    camp, scenario, tex, prefix = sys.argv[1:5]
    runs = json.load(open(os.path.join(camp, "datadog.json")))["runs"]
    reps = sorted(int(k.split("/")[1]) for k in runs if k.split("/")[0] == scenario)
    mac = {"Runs": str(len(reps))}
    for band, a, b in PAIRS:
        without = [runs[f"{scenario}/{r}"][a][0] for r in reps]
        with_ = [runs[f"{scenario}/{r}"][b][0] for r in reps]
        diff = [y - x for x, y in zip(without, with_)]
        mean, lo, hi = sigsum.interval(diff)
        slots = [(runs[f"{scenario}/{r}"][b][2] - runs[f"{scenario}/{r}"][b][3]) - (runs[f"{scenario}/{r}"][a][2] - runs[f"{scenario}/{r}"][a][3]) for r in reps]
        smean, slo, shi = sigsum.interval(slots)
        w, v = sum(without) / len(without), sum(with_) / len(with_)
        mac.update({f"{band}Without": f"{w:.0f}", f"{band}With": f"{v:.0f}", f"{band}Diff": f"{-mean:.0f}", f"{band}DiffLo": f"{-hi:.0f}",
                    f"{band}DiffHi": f"{-lo:.0f}", f"{band}Lower": str(sum(x < 0 for x in diff)), f"{band}Share": f"{100 * (1 - v / w):.0f}",
                    f"{band}P": f"{sigsum.sign_flip_p(diff):.3f}", f"{band}SlotsDiff": f"{smean:.1f}", f"{band}SlotsLo": f"{slo:.1f}",
                    f"{band}SlotsHi": f"{shi:.1f}"})
        print(f"{scenario} {band}: {w:.1f} s without, {v:.1f} s with; with minus without {mean:+.1f} [{lo:+.1f}, {hi:+.1f}], lower in "
              f"{sum(x < 0 for x in diff)} of {len(diff)}, p {sigsum.sign_flip_p(diff):.4f}; slots {smean:+.2f} [{slo:+.2f}, {shi:+.2f}]")
    with open(tex, "w") as f:
        f.write(f"% written by cluster/bench/steptex.py from {camp}, scenario {scenario}. The differences are 'without minus with'.\n")
        for k in sorted(mac):
            value = mac[k]
            if value.isdigit() and len(value) > 3:
                value = f"{int(value):,}".replace(",", "{,}")
            f.write(f"\\newcommand{{\\{prefix}{k}}}{{{value}}}\n")
    print(f"wrote {len(mac)} macros to {tex}")


if __name__ == "__main__":
    main()

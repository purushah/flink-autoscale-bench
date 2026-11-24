"""The wiring inside run_scenario, which needs a cluster to run: checked on the source and through the
functions it calls (review of 23 Nov: I1, I8, I2)."""
import ast
import inspect
import os
import re
import sys

import bench
from scenarios import SCENARIOS

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
sys.path.insert(0, os.path.join(ROOT, "cluster", "bench"))
import bench_cluster  # noqa: E402


def bindings_of_the_start_entry(module):
    """How often run_scenario binds the name it writes to meta.json as "start"."""
    tree = ast.parse(inspect.getsource(module.run_scenario))
    name = next(v.id for d in ast.walk(tree) if isinstance(d, ast.Dict)
                for k, v in zip(d.keys, d.values) if isinstance(k, ast.Constant) and k.value == "start" and isinstance(v, ast.Name))
    stores = [n for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Store)]
    return name, len(stores)


def test_the_start_state_is_not_overwritten_by_another_value_in_the_laptop_harness():
    # With --fault the tuple (kind, start, length) used to rebind it: meta["start"] became the fault's start time.
    name, stores = bindings_of_the_start_entry(bench)
    assert stores == 1, f"{name} is bound {stores} times in run_scenario"


def test_the_start_state_is_not_overwritten_in_the_cluster_harness():
    assert bindings_of_the_start_entry(bench_cluster)[1] == 1


def test_a_job_with_a_fixed_start_size_is_compared_with_that_size():
    s = SCENARIOS["spike"]
    aggregate, measure = bench.initial_parallelism(s, bench.autoscaler_conf("builtin"))
    assert bench.start_sizes("builtin", s) == (aggregate, measure)
    assert bench.start_sizes("builtin+agg8", s) == (8, measure)
    assert bench.start_sizes("builtin+agg8+off", s) == (8, 1)
    args = bench.job_args("builtin+agg8", "x", s)
    assert args[args.index("--agg-parallelism") + 1] == "8" and args[args.index("--measure-parallelism") + 1] == str(measure)


def test_both_harnesses_check_the_start_against_the_start_sizes():
    for module in (bench, bench_cluster):
        source = inspect.getsource(module.run_scenario)
        assert re.search(r"start_state\(sampler\.sizes\(\), wanted\)", source), module.__name__
        assert "start_sizes(" in source, module.__name__


def test_the_driver_image_gets_every_module_the_harness_imports():
    local = {f[:-3] for f in os.listdir(os.path.join(ROOT, "bench")) if f.endswith(".py")}
    needed = set()
    for path in (os.path.join(ROOT, "bench", "bench.py"), os.path.join(ROOT, "cluster", "bench", "bench_cluster.py")):
        for node in ast.walk(ast.parse(open(path).read())):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module] if isinstance(node, ast.ImportFrom) else []
            needed |= {n for n in names if n in local}
    build = open(os.path.join(ROOT, "cluster", "build.sh")).read()
    missing = sorted(m for m in needed if f"bench/{m}.py" not in build)
    assert not missing, f"cluster/build.sh does not copy {missing} into the driver image"

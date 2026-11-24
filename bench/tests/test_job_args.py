"""Job specs and the program arguments they give a job."""
import bench


def after(args, flag):
    return args[args.index(flag) + 1]


def test_rr_delays_restores_only():
    args = bench.job_args("rule+rr60", "x")
    assert after(args, "--restore-delay-ms") == "60000"
    assert after(args, "--restore-delay-first-start") == "false"


def test_rd_still_delays_every_start():
    args = bench.job_args("rule+rd60", "x")
    assert after(args, "--restore-delay-ms") == "60000"
    assert "--restore-delay-first-start" not in args


def test_a_job_without_either_has_no_delay():
    assert "--restore-delay-ms" not in bench.job_args("rule", "x")


def test_the_spec_grammar_knows_rr():
    assert bench.SPEC.fullmatch("builtin+max70+rr60+rt70")
    assert bench.options("builtin+rr60") == {"rr": 60}


def test_the_spec_grammar_knows_the_three_advisor_options():
    for option in ("advc", "advl", "advd"):
        assert bench.SPEC.fullmatch(f"builtin+max70+{option}")
        assert bench.options(f"builtin+{option}") == {option: True}


def test_an_advisor_option_changes_nothing_in_the_jobs_configuration():
    assert bench.autoscaler_conf("builtin+advl") == bench.autoscaler_conf("builtin")
    assert bench.autoscaler_conf("builtin+max70+advc") == bench.autoscaler_conf("builtin+max70")

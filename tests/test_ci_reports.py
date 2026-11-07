"""CI annotations retain traceback evidence without breaking workflow syntax."""

from scripts import report_ci_failures


def test_nested_junit_failures_include_escaped_tracebacks_and_locations(tmp_path, capsys):
    path = tmp_path / "results.xml"
    path.write_text(
        '<testsuites><testsuite><testcase classname="tests.test_example" name="test_case[a,b]" '
        'file="tests/test_example.py" line="9"><failure message="summary">'
        "Traceback:\nassert 100% == 0\nAssertionError: full evidence"
        '</failure></testcase><testcase name="setup"><error>fixture exploded</error></testcase>'
        '<testcase name="passed"/></testsuite></testsuites>'
    )
    assert report_ci_failures.main([str(tmp_path)]) == 0
    output = capsys.readouterr().out
    assert "::error file=tests/test_example.py,line=10,title=tests.test_example.test_case[a%2Cb]::" in output
    assert "Traceback:%0Aassert 100%25 == 0%0AAssertionError: full evidence" in output
    assert "::error title=setup::fixture exploded" in output
    assert "Reported 2 JUnit failure annotations." in output


def test_missing_and_invalid_reports_do_not_hide_the_original_ci_failure(tmp_path, capsys):
    assert report_ci_failures.main([str(tmp_path / "missing")]) == 0
    assert "Reported 0" in capsys.readouterr().out
    broken = tmp_path / "broken.xml"
    broken.write_text("not XML")
    assert report_ci_failures.main([str(broken)]) == 0
    assert "::warning::Cannot read JUnit report" in capsys.readouterr().out

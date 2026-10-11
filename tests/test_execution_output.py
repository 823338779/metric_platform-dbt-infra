"""脱敏在截断前完成，并覆盖跨块秘密及 UTF-8 边界。"""

import pytest

from dbt_metricflow_service.execution.output import _RedactingTailBuffer


@pytest.mark.parametrize("split", range(1, 19))
def test_secret_redaction_across_utf8_and_overlapping_chunks(split):
    content = "前缀-private-secret-后缀".encode()
    output = _RedactingTailBuffer(128, ["private-secret", "secret"])
    output.feed(content[:split])
    output.feed(content[split:])
    output.finish()
    assert output.decode() == "前缀-***-后缀"
    assert not output.truncated


def test_secret_is_removed_before_tail_truncation():
    output = _RedactingTailBuffer(5, ["long-private-secret"])
    output.feed(b"prefix-long-private-secret!")
    output.finish()
    assert output.decode() == "-***!"
    assert output.truncated


def test_plain_output_retains_bounded_tail():
    output = _RedactingTailBuffer(4, [])
    output.feed(b"01234567")
    output.finish()
    assert output.decode() == "4567"
    assert output.truncated

import pytest, asyncio
from lanscatter import planner

@pytest.mark.timeout(60)
def test_planner(capsys):

    planner.simulate()
    captured = capsys.readouterr()
    assert 'ALL DONE' in captured.out
    assert 'xception' not in captured.out
    assert 'xception' not in captured.err

import pytest

from orchestration.batch_partition import select_group, total_groups


def test_total_groups_rounds_up() -> None:
    assert total_groups(0, 10) == 0
    assert total_groups(1, 10) == 1
    assert total_groups(10, 10) == 1
    assert total_groups(11, 10) == 2


def test_select_group_returns_expected_slice() -> None:
    items = list(range(10))
    assert select_group(items, group_size=3, group_index=0) == [0, 1, 2]
    assert select_group(items, group_size=3, group_index=1) == [3, 4, 5]
    assert select_group(items, group_size=3, group_index=3) == [9]
    assert select_group(items, group_size=3, group_index=4) == []


def test_select_group_validates_inputs() -> None:
    with pytest.raises(ValueError, match="group_size"):
        select_group([1, 2, 3], group_size=0, group_index=0)
    with pytest.raises(ValueError, match="group_index"):
        select_group([1, 2, 3], group_size=2, group_index=-1)

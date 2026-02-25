from __future__ import annotations

from math import ceil
from typing import Sequence, TypeVar

T = TypeVar("T")


def total_groups(total_items: int, group_size: int) -> int:
    if group_size <= 0:
        raise ValueError("group_size must be greater than 0")
    if total_items <= 0:
        return 0
    return ceil(total_items / group_size)


def select_group(items: Sequence[T], group_size: int, group_index: int) -> list[T]:
    if group_size <= 0:
        raise ValueError("group_size must be greater than 0")
    if group_index < 0:
        raise ValueError("group_index must be greater than or equal to 0")

    start = group_index * group_size
    end = start + group_size
    if start >= len(items):
        return []
    return list(items[start:end])

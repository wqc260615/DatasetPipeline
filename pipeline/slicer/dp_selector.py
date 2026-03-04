"""Dynamic-programming selector for tag anchors."""

import logging
import math
from typing import List, Optional, Sequence


module_logger = logging.getLogger(__name__)


def select_tag_slices_dp(
    anchors: Sequence[object],
    distances: List[float],
    n: int,
    gain_func: str = "log1p",
    force_first: bool = True,
    logger: Optional[logging.Logger] = None,
) -> List[int]:
    """
    Select *n* anchor indices that maximise total segment gain.
    """
    log = logger or module_logger

    m = len(anchors)
    if m == 0:
        return []
    if n >= m:
        log.info(
            f"target_slices ({n}) >= anchor count ({m}) – selecting all anchors"
        )
        return list(range(m))
    if n <= 0:
        return []
    if n == 1:
        return [0] if force_first else [m - 1]

    if gain_func == "sqrt":
        _g = math.sqrt
    else:
        _g = math.log1p

    prefix = [0.0] * m
    for i in range(len(distances)):
        prefix[i + 1] = prefix[i] + distances[i]

    def seg(i: int, j: int) -> float:
        return prefix[j] - prefix[i]

    neg_inf = float("-inf")
    dp = [[neg_inf] * (n + 1) for _ in range(m)]
    parent = [[-1] * (n + 1) for _ in range(m)]

    if force_first:
        dp[0][1] = 0.0
    else:
        for j in range(m):
            dp[j][1] = 0.0

    for k in range(2, n + 1):
        for j in range(k - 1, m):
            for i in range(j):
                if dp[i][k - 1] == neg_inf:
                    continue
                gain = dp[i][k - 1] + _g(seg(i, j))
                if gain > dp[j][k]:
                    dp[j][k] = gain
                    parent[j][k] = i

    best_gain = neg_inf
    last_idx = -1
    for j in range(n - 1, m):
        if dp[j][n] > best_gain:
            best_gain = dp[j][n]
            last_idx = j

    if last_idx == -1:
        log.warning("DP found no valid selection – returning first n anchors")
        return list(range(n))

    selected: List[int] = []
    j = last_idx
    for k in range(n, 0, -1):
        selected.append(j)
        j = parent[j][k]

    selected.reverse()

    log.info(
        f"DP total gain = {best_gain:.4f} "
        f"(gain_func={gain_func}, n={n}, anchors={m})"
    )
    return selected

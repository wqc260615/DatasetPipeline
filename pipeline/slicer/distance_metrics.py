"""Distance normalization helpers for tag-pair metrics."""

from typing import Any, List, Type

from pipeline.config import SlicingConfig


def percentile_rank(value: float, population: List[float]) -> float:
    """
    Compute the percentile rank of *value* within *population*.

    .. math::

        P(x) = \\frac{|\\{v \\in V : v \\le x\\}|}{|V|}
    """
    n = len(population)
    if n == 0:
        return 0.0
    count_le = sum(1 for v in population if v <= value)
    return count_le / n


def normalize_tag_pair_metrics(
    metrics: List[Any],
    config: SlicingConfig,
    normalized_metric_cls: Type[Any],
) -> List[Any]:
    """
    Normalise raw metrics via percentile rank and compute weighted distance.
    """
    if not metrics:
        return []

    w = config.distance_weights

    lines_values = [m.delta_lines for m in metrics]
    files_values = [m.delta_files for m in metrics]

    normalised: List[Any] = []
    for m in metrics:
        norm_l = percentile_rank(m.delta_lines, lines_values)
        norm_f = percentile_rank(m.delta_files, files_values)
        dist = w.lines * norm_l + w.files * norm_f + w.api_break * m.api_break

        normalised.append(normalized_metric_cls(
            from_anchor=m.from_anchor,
            to_anchor=m.to_anchor,
            delta_lines=m.delta_lines,
            delta_files=m.delta_files,
            api_break=m.api_break,
            norm_lines=norm_l,
            norm_files=norm_f,
            distance=dist,
        ))

    return normalised

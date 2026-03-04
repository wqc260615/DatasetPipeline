"""SemVer comparison utilities for release tag ordering."""

from pipeline.commit_extractor import parse_version_tag


def compare_prerelease_identifiers(a: str, b: str) -> int:
    """Compare SemVer 2.0.0 prerelease identifiers."""
    a_parts = a.split(".")
    b_parts = b.split(".")

    for a_part, b_part in zip(a_parts, b_parts):
        a_is_num = a_part.isdigit()
        b_is_num = b_part.isdigit()

        if a_is_num and b_is_num:
            a_num, b_num = int(a_part), int(b_part)
            if a_num != b_num:
                return -1 if a_num < b_num else 1
            continue

        if a_is_num and not b_is_num:
            return -1
        if not a_is_num and b_is_num:
            return 1

        if a_part != b_part:
            return -1 if a_part < b_part else 1

    if len(a_parts) != len(b_parts):
        return -1 if len(a_parts) < len(b_parts) else 1

    return 0


def compare_version_tags(tag_a: str, tag_b: str) -> int:
    """
    Compare two semantic version tags by SemVer precedence.

    Returns -1, 0, or 1.
    """
    version_a = parse_version_tag(tag_a)
    version_b = parse_version_tag(tag_b)
    if version_a is None:
        raise ValueError(f"Invalid version tag: {tag_a}")
    if version_b is None:
        raise ValueError(f"Invalid version tag: {tag_b}")

    a_core = (version_a["major"], version_a["minor"], version_a["patch"])
    b_core = (version_b["major"], version_b["minor"], version_b["patch"])
    if a_core != b_core:
        return -1 if a_core < b_core else 1

    a_pre = version_a.get("prerelease")
    b_pre = version_b.get("prerelease")
    if a_pre is None and b_pre is None:
        return 0
    if a_pre is None:
        return 1
    if b_pre is None:
        return -1

    return compare_prerelease_identifiers(a_pre, b_pre)

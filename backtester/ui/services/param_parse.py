"""
services/param_parse.py — PARAM_GRID CSV / range shorthand parsing for New Run.
"""


def expand_range_token(token: str) -> list:
    """Parse a range shorthand token ``start..end[:step]`` into a list of values.

    Uses integer arithmetic when all of *start*, *end*, and *step* are written
    without decimal points; float arithmetic otherwise.

    Examples::

        expand_range_token("10..50:5")    → [10, 15, 20, 25, 30, 35, 40, 45, 50]
        expand_range_token("0.1..0.5:0.1") → [0.1, 0.2, 0.3, 0.4, 0.5]
        expand_range_token("3..7")         → [3, 4, 5, 6, 7]

    Raises:
        ValueError: if step is not > 0, or start > end, or no values produced.
    """
    token = token.strip()
    step_raw: str | None = None

    if ":" in token:
        range_part, step_raw = token.rsplit(":", 1)
        step_raw = step_raw.strip()
    else:
        range_part = token

    if ".." not in range_part:
        raise ValueError(f"not a range token: '{token}'")

    start_raw, end_raw = range_part.split("..", 1)
    start_raw = start_raw.strip()
    end_raw = end_raw.strip()

    # Decide int vs float based on presence of '.' in any component
    use_int = (
        "." not in start_raw
        and "." not in end_raw
        and (step_raw is None or "." not in step_raw)
    )

    if use_int:
        start = int(start_raw)
        end = int(end_raw)
        step = int(step_raw) if step_raw else 1
        if step <= 0:
            raise ValueError(f"step must be > 0, got {step}")
        if start > end:
            raise ValueError(f"start ({start}) > end ({end})")
        result = list(range(start, end + 1, step))
    else:
        start = float(start_raw)
        end = float(end_raw)
        step = float(step_raw) if step_raw else 1.0
        if step <= 0.0:
            raise ValueError(f"step must be > 0, got {step}")
        if start > end + 1e-9 * abs(step):
            raise ValueError(f"start ({start}) > end ({end})")
        # Determine decimal precision from step (or start if no step given)
        _src = step_raw if step_raw else start_raw
        if "." in _src:
            _decimals = len(_src.rstrip("0").split(".")[-1])
        else:
            _decimals = 6
        n = int(round((end - start) / step)) + 1
        result = []
        for i in range(n):
            v = round(start + i * step, _decimals + 1)
            if v > end + step * 1e-9:
                break
            result.append(round(v, _decimals))

    if not result:
        raise ValueError(f"range '{token}' produces no values")
    return result


# Back-compat alias used by older tests
_expand_range_token = expand_range_token


def parse_param_csv(key: str, csv_str: str, sample) -> tuple:
    """Parse a CSV string into a typed list.

    Args:
        key:     Param name (used only in error messages).
        csv_str: Comma-separated value string e.g. "0, 3.0, 6.0".
        sample:  A representative value from the strategy's PARAM_GRID
                 (used to infer target type).

    Returns:
        (values, error_msg) — values is a list if successful, None on error.
        error_msg is None on success.
    """
    parts = [p.strip() for p in csv_str.split(",") if p.strip()]
    if not parts:
        return None, f"{key}: at least one value required"
    try:
        # First pass: expand range tokens so the rest of the logic is uniform
        expanded: list = []
        for p in parts:
            if ".." in p:
                expanded.extend(expand_range_token(p))
            else:
                expanded.append(p)  # raw string; type-coerced below

        if isinstance(sample, bool):
            result = []
            for item in expanded:
                s = str(item).lower() if not isinstance(item, bool) else ("true" if item else "false")
                if s in ("1", "true", "yes"):
                    result.append(True)
                elif s in ("0", "false", "no"):
                    result.append(False)
                else:
                    raise ValueError(f"expected bool, got '{item}'")
            return result, None
        elif isinstance(sample, int):
            result = []
            for item in expanded:
                if isinstance(item, (int, float)) and not isinstance(item, bool):
                    result.append(item)  # keep native type from range expansion
                else:
                    try:
                        result.append(int(str(item)))
                    except ValueError:
                        result.append(float(str(item)))
            return result, None
        elif isinstance(sample, float):
            return [float(item) for item in expanded], None
        else:
            return [str(item) for item in expanded], None
    except (ValueError, TypeError) as exc:
        return None, f"{key}: {exc}"


def csv_from_values(values) -> str:
    """Convert a list of param values back to a CSV string."""
    return ", ".join(str(v) for v in values)

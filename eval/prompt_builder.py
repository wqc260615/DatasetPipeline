"""Build LLM prompts for each QA type (intrinsic, extrinsic, temporal)."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Intrinsic
# ---------------------------------------------------------------------------

_INTRINSIC_TEMPLATE = """\
You are a code analysis assistant. Given the source code of a file, answer the \
question precisely and concisely.

File: {file_path}
```python
{content}
```

Question: {question}

{format_hint}"""

# Subtype-specific answer format instructions.
# The model often outputs full signatures or code blocks when a concise value
# is expected, so we spell out the exact expected format per subtype.
_INTRINSIC_FORMAT_HINTS: dict[str, str] = {
    "function_signature": (
        "Answer with only the `def` or `async def` line itself (include `async` if the "
        "function is async). Do NOT include decorators. Do not wrap in code blocks."
    ),
    "function_parameters": (
        "Answer with only the parameter list in parentheses exactly as it appears in the "
        "function definition, e.g. `(x, y)` or `(x: int, y: str = 'default')`. "
        "Do NOT include `def`, the function name, `->`, or the return type."
    ),
    "symbol_callers": (
        "Answer with only the function/method names this function calls internally, "
        "comma-separated (e.g. `foo, obj.bar`). No extra explanation."
    ),
    "object_instantiations": (
        "Answer with only the class name(s) instantiated inside this function, "
        "comma-separated (e.g. `Foo, Bar`). No extra explanation."
    ),
    "field_accesses": (
        "Answer with only the field/attribute access expressions, comma-separated "
        "(e.g. `self.attr1, obj.field2`). No extra explanation."
    ),
    "symbol_location": (
        "Answer with only the file path where the symbol is defined. No explanation."
    ),
    "class_inheritance": (
        "Answer with only the base class name(s), comma-separated. "
        "If none, answer `none`."
    ),
    "class_fields": (
        "Answer with only the field/attribute names, comma-separated. No explanation."
    ),
    "class_instantiation_sites": (
        "Answer with only the function/method name(s) where this class is instantiated, "
        "comma-separated. No explanation."
    ),
}

_INTRINSIC_DEFAULT_HINT = (
    "Answer with the exact text only. No explanation, no markdown formatting. "
    "Do not wrap the answer in code blocks."
)

# ---------------------------------------------------------------------------
# Extrinsic
# ---------------------------------------------------------------------------

_EXTRINSIC_TEMPLATE = """\
You are a code documentation assistant. Answer the question based solely on \
the docstrings and comments found in the source code below.

File: {file_path}
```python
{content}
```

Question: {question}

Answer concisely based on what the documentation says. Do not elaborate beyond what the docs state."""

_EXTRINSIC_YESNO_TEMPLATE = """\
You are a code documentation assistant. Answer the question based solely on \
the docstrings and comments found in the source code below.

File: {file_path}
```python
{content}
```

Question: {question}

Answer with only "Yes" or "No". No explanation."""

# ---------------------------------------------------------------------------
# Temporal — binary (Yes / No) subtypes
# ---------------------------------------------------------------------------

_TEMPORAL_YESNO_SUBTYPES = {
    "function_introduced", "function_not_introduced",
    "function_removed", "function_not_removed",
    "function_signature_unchanged", "function_return_type_unchanged",
    "class_introduced", "class_not_introduced",
    "class_removed", "class_not_removed",
    "class_inheritance_unchanged",
}

_TEMPORAL_YESNO_TEMPLATE = """\
You are a code evolution analyst. Two versions of a source file are provided. \
Determine whether the described change occurred between them.

=== Version A ({from_version}) ===
File: {file_path}
```python
{from_content}
```

=== Version B ({to_version}) ===
File: {file_path}
```python
{to_content}
```

Question: {question}

Answer only with "Yes" or "No". No explanation, no markdown."""

# ---------------------------------------------------------------------------
# Temporal — "how did X change" subtypes  (answer: "from: X ; to: Y")
# ---------------------------------------------------------------------------

_TEMPORAL_CHANGED_SUBTYPES = {
    "function_signature_changed",
    "function_return_type_changed",
    "class_inheritance_changed",
    "function_calls_changed",
    "function_instantiations_changed",
}

_TEMPORAL_CHANGED_TEMPLATE = """\
You are a code evolution analyst. Two versions of a source file are provided.

=== Version A ({from_version}) ===
File: {file_path}
```python
{from_content}
```

=== Version B ({to_version}) ===
File: {file_path}
```python
{to_content}
```

Question: {question}

Answer in the exact format: `from: <old value> ; to: <new value>`
Use the exact signature/type/value as it appears in the code. No extra explanation."""

# ---------------------------------------------------------------------------
# Temporal — ordering subtypes  (answer: version string)
# ---------------------------------------------------------------------------

_TEMPORAL_VERSION_SUBTYPES = {
    "function_first_introduced",
    "function_last_present",
    "class_first_introduced",
    "class_last_present",
}

_TEMPORAL_VERSION_TEMPLATE = """\
You are a code evolution analyst. Two file snapshots spanning a version range are provided.

=== Earliest snapshot ({from_version}) ===
File: {file_path}
```python
{from_content}
```

=== Latest snapshot ({to_version}) ===
File: {file_path}
```python
{to_content}
```

Question: {question}

Answer with only the version string (e.g. `0.75.2`). No explanation, no markdown."""

# ---------------------------------------------------------------------------
# Temporal — evolution subtypes  (answer: full trajectory)
# ---------------------------------------------------------------------------

_TEMPORAL_EVOLUTION_TEMPLATE = """\
You are a code evolution analyst. Two file snapshots spanning a version range are provided.

=== Earliest snapshot ({from_version}) ===
File: {file_path}
```python
{from_content}
```

=== Latest snapshot ({to_version}) ===
File: {file_path}
```python
{to_content}
```

Question: {question}

Answer in the format: `<version>: <value> -> <version>: <value> -> ...`
List each distinct value with the version it first appeared. No extra explanation."""

_FILE_MISSING = "# [File did not exist in this version]"


def build_prompt(qa_pair: dict, context: dict) -> str:
    """Return the prompt string for a QA pair given its retrieved context."""
    qa_type = qa_pair["qa_type"]
    qa_subtype = qa_pair.get("qa_subtype", "")
    question = qa_pair["question"]
    file_path = context["file_path"]

    if qa_type == "temporal":
        kwargs = dict(
            from_version=context.get("from_version", "version A"),
            to_version=context.get("to_version", "version B"),
            file_path=file_path,
            from_content=context.get("from_content") or _FILE_MISSING,
            to_content=context.get("to_content") or _FILE_MISSING,
            question=question,
        )
        if qa_subtype in _TEMPORAL_YESNO_SUBTYPES:
            return _TEMPORAL_YESNO_TEMPLATE.format(**kwargs)
        elif qa_subtype in _TEMPORAL_CHANGED_SUBTYPES:
            return _TEMPORAL_CHANGED_TEMPLATE.format(**kwargs)
        elif qa_subtype in _TEMPORAL_VERSION_SUBTYPES:
            return _TEMPORAL_VERSION_TEMPLATE.format(**kwargs)
        else:
            # evolution or unknown subtypes
            return _TEMPORAL_EVOLUTION_TEMPLATE.format(**kwargs)

    elif qa_type == "extrinsic":
        content = context.get("content") or _FILE_MISSING
        if qa_subtype == "yesno":
            return _EXTRINSIC_YESNO_TEMPLATE.format(
                file_path=file_path, content=content, question=question
            )
        return _EXTRINSIC_TEMPLATE.format(
            file_path=file_path, content=content, question=question
        )

    else:  # intrinsic
        hint = _INTRINSIC_FORMAT_HINTS.get(qa_subtype, _INTRINSIC_DEFAULT_HINT)
        return _INTRINSIC_TEMPLATE.format(
            file_path=file_path,
            content=context.get("content") or _FILE_MISSING,
            question=question,
            format_hint=hint,
        )

"""Build LLM prompts for each QA type (intrinsic, extrinsic, temporal)."""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

_LANG_MAP: dict[str, str] = {
    ".py": "python",
    ".java": "java",
}


def _get_lang(file_path: str) -> str:
    """Return a Markdown code-fence language tag from a file path."""
    return _LANG_MAP.get(Path(file_path).suffix.lower(), "")


# ---------------------------------------------------------------------------
# Intrinsic
# ---------------------------------------------------------------------------

_INTRINSIC_TEMPLATE = """\
You are a code analysis assistant. Given the source code of a file, answer the \
question precisely and concisely.

File: {file_path}
```{lang}
{content}
```

Question: {question}

{format_hint}"""

# Multi-file variant used for cross-file subtypes (class_subclasses, class_instantiation_sites)
_INTRINSIC_MULTIFILE_TEMPLATE = """\
You are a code analysis assistant. Given the source code of several files from \
the same codebase, answer the question precisely.

Primary file:
File: {file_path}
```{lang}
{content}
```
{related_sections}
Question: {question}

{format_hint}"""

_RELATED_FILE_SECTION = """\
Related file:
File: {file_path}
```{lang}
{content}
```
"""

# Subtype-specific answer format instructions.
# The model often outputs full signatures or code blocks when a concise value
# is expected, so we spell out the exact expected format per subtype.
_INTRINSIC_FORMAT_HINTS: dict[str, str] = {
    # function_signature is language-specific — resolved at prompt-build time via
    # _function_signature_hint(); this entry is a fallback only.
    "function_signature": (
        "Answer with the full method signature as it appears in the source code. "
        "Do NOT include the body or decorators. Do not wrap in code blocks. "
        "You MUST provide the full signature line — do not leave blank."
    ),
    "function_parameters": (
        "Answer with only the parameter list in parentheses exactly as it appears in the "
        "function definition, e.g. `(x, y)` or `(x: int, y: str = 'default')`. "
        "Do NOT include `def`, the function name, `->`, or the return type."
    ),
    "symbol_callers": (
        "Answer with only the bare method/function names this function calls internally, "
        "comma-separated. Use the unqualified name only — do NOT include the class or "
        "object prefix (e.g. write `sort` not `Collections.sort`, write `run` not "
        "`SpringApplication.run`). "
        "If the function calls nothing, answer `none`. No extra explanation."
    ),
    "object_instantiations": (
        "Answer with only the class name(s) instantiated inside this function, "
        "comma-separated (e.g. `Foo, Bar`). Include classes raised as exceptions "
        "(e.g. `raise HTTPException(...)` counts as `HTTPException`). "
        "If none, answer `none`. No extra explanation."
    ),
    "field_accesses": (
        "Answer with only the dotted-access expressions this function uses, comma-separated. "
        "Include all patterns of the form `obj.attr` or `obj.method` (e.g. `self.x`, `results.update`, `commons.q`). "
        "Do NOT include the call parentheses. If none, answer `none`. No extra explanation."
    ),
    "symbol_location": (
        "Answer with only the file path where the symbol is defined. No explanation."
    ),
    "class_inheritance": (
        "Answer with only the base class name(s), comma-separated (use the full name as written "
        "in the class definition, e.g. `routing.WebSocketRoute`). "
        "If none, answer `none`. Do not leave blank."
    ),
    "class_fields": (
        "Answer with only the field/attribute names, comma-separated. No explanation."
    ),
    "class_subclasses": (
        "Answer with only the subclass name(s) visible in the provided files, "
        "comma-separated (e.g. `Foo, Bar`). "
        "If no subclass is shown in the provided source files, answer `none`. No explanation."
    ),
    "class_instantiation_sites": (
        "Answer with only the methods that instantiate this class, in `ClassName.methodName` "
        "format, comma-separated (e.g. `MyFactory.create, Builder.build`). "
        "If none are visible in the provided source files, answer `none`. No explanation."
    ),
}

_JAVA_SIGNATURE_HINT = (
    "Answer with the full method signature in this exact format: "
    "`<modifiers> <ReturnType> <MethodName>(<paramName: ParamType, ...>) -> <ReturnType>:` "
    "(e.g. `public void doThing(input: String) -> void:` or "
    "`public static List<Foo> getAll(id: int) -> List<Foo>:`). "
    "Use the exact Java modifiers, return type, and type names from the source. "
    "Parameters use `name: Type` order (name first, colon, then type). "
    "For constructors, use `<modifiers> <ClassName>(<params>):` with no `->` section. "
    "Do NOT use Python `def` syntax. Do NOT wrap in code blocks."
)

_PYTHON_SIGNATURE_HINT = (
    "Answer with only the `def` or `async def` line itself (include `async` if the "
    "function is async). Do NOT include decorators. Do not wrap in code blocks. "
    "You MUST provide the full signature line — do not leave blank."
)

_GENERIC_SIGNATURE_HINT = (
    "Answer with the full method/function signature line as it appears in the source. "
    "Do NOT include the body, decorators, or annotations. Do not wrap in code blocks. "
    "You MUST provide the full signature line — do not leave blank."
)


def _function_signature_hint(lang: str) -> str:
    """Return the appropriate format hint for function_signature based on language."""
    if lang == "python":
        return _PYTHON_SIGNATURE_HINT
    elif lang == "java":
        return _JAVA_SIGNATURE_HINT
    return _GENERIC_SIGNATURE_HINT

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

Answer concisely based on what the documentation says. Do not elaborate beyond what the docs state. If no relevant docstring or comment is found, answer `[no documentation]`."""

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

Answer only with "Yes" or "No". You MUST provide an answer — do not leave it blank. No explanation, no markdown."""

# ---------------------------------------------------------------------------
# Temporal — "how did X change" subtypes  (answer: "from: X ; to: Y")
# ---------------------------------------------------------------------------

_TEMPORAL_CHANGED_SUBTYPES = {
    "function_signature_changed",
    "function_return_type_changed",
    "class_inheritance_changed",
    "function_instantiations_changed",
}

# For most "changed" questions: just ask for exact before/after.
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

# For function_calls_changed: answer with added/removed diff to match ground-truth format.
_TEMPORAL_CALLS_CHANGED_TEMPLATE = """\
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

Compare the set of functions/methods called inside the named function between the two versions.
Answer using ONLY this exact format: `added: f1, f2; removed: f3`
Omit "added: ..." if no calls were added. Omit "removed: ..." if no calls were removed.
Use bare names only (no class/object prefix). No extra explanation."""

# For return-type questions the value may not be annotated: use 'unknown' in that case.
_TEMPORAL_RETURN_TYPE_CHANGED_TEMPLATE = """\
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
If a return type is not annotated in the code, write `unknown` for that side. No extra explanation."""

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
You are a code evolution analyst. The source file snapshot shown below is from \
the exact version where the described change boundary occurred — either the \
first version where the symbol was introduced, or the last version where it was \
still present.

=== Snapshot ({from_version}) ===
File: {file_path}
```{lang}
{from_content}
```

Question: {question}

Answer with only the version string shown in the snapshot header above \
(e.g. `v2.2.15`). Do not leave blank, no other explanation."""

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

# Returned by build_prompt when context is completely unavailable.
# The evaluator should skip LLM inference and use this string as the prediction.
CONTEXT_UNAVAILABLE = "[context unavailable]"


def build_prompt(qa_pair: dict, context: dict) -> str | None:
    """Return the prompt string for a QA pair given its retrieved context.

    Returns ``None`` (i.e. ``CONTEXT_UNAVAILABLE`` sentinel is used by caller)
    when ALL source code is missing so the LLM would have nothing to reason over.
    """
    qa_type = qa_pair["qa_type"]
    qa_subtype = qa_pair.get("qa_subtype", "")
    question = qa_pair["question"]
    file_path = context["file_path"]

    # Early-exit when context retrieval failed completely
    if qa_type == "temporal":
        if context.get("from_content") is None and context.get("to_content") is None:
            return None
    else:  # intrinsic / extrinsic
        if context.get("content") is None:
            return None

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
        elif qa_subtype == "function_calls_changed":
            return _TEMPORAL_CALLS_CHANGED_TEMPLATE.format(**kwargs)
        elif qa_subtype == "function_return_type_changed":
            return _TEMPORAL_RETURN_TYPE_CHANGED_TEMPLATE.format(**kwargs)
        elif qa_subtype in _TEMPORAL_CHANGED_SUBTYPES:
            return _TEMPORAL_CHANGED_TEMPLATE.format(**kwargs)
        elif qa_subtype in _TEMPORAL_VERSION_SUBTYPES:
            return _TEMPORAL_VERSION_TEMPLATE.format(
                from_version=context.get("from_version", "unknown version"),
                file_path=file_path,
                lang=_get_lang(file_path),
                from_content=context.get("from_content") or _FILE_MISSING,
                question=question,
            )
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
        lang = _get_lang(file_path)
        if qa_subtype == "function_signature":
            hint = _function_signature_hint(lang)
        else:
            hint = _INTRINSIC_FORMAT_HINTS.get(qa_subtype, _INTRINSIC_DEFAULT_HINT)
        content = context.get("content") or _FILE_MISSING
        related_contents: list = context.get("related_contents") or []
        if related_contents:
            related_sections = ""
            for rc in related_contents:
                rl = _get_lang(rc["file_path"])
                related_sections += _RELATED_FILE_SECTION.format(
                    file_path=rc["file_path"],
                    lang=rl,
                    content=rc["content"],
                )
            return _INTRINSIC_MULTIFILE_TEMPLATE.format(
                file_path=file_path,
                lang=lang,
                content=content,
                related_sections=related_sections,
                question=question,
                format_hint=hint,
            )
        return _INTRINSIC_TEMPLATE.format(
            file_path=file_path,
            lang=lang,
            content=content,
            question=question,
            format_hint=hint,
        )

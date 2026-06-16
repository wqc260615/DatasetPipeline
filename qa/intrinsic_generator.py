"""Generate intrinsic QA pairs for one slice context."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from qa.qa_common import format_params, make_qa, public_class, public_function, symbol_ref
from qa.qa_types import SliceContext


def _complete_signature(sig: str, params: str, return_type: str) -> str:
    """Return a complete, colon-terminated signature.

    When the stored signature was truncated at the opening parenthesis (e.g.
    ``async def foo(`` from a multi-line definition), reconstruct it from the
    already-parsed *params* string and optional *return_type*.
    """
    if sig.rstrip().endswith(":"):
        return sig  # already complete
    paren_idx = sig.find("(")
    prefix = sig[:paren_idx] if paren_idx != -1 else sig
    result = f"{prefix.rstrip()}{params}"
    if return_type:
        result += f" -> {return_type}"
    return result + ":"


def build_intrinsic_qas(
    ctx: SliceContext,
    neg_func_refs: Optional[List[str]] = None,
    neg_class_names: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    qas: List[Dict[str, Any]] = []
    public_functions = [f for f in ctx.functions if public_function(f)]
    public_classes = [c for c in ctx.classes if public_class(c)]
    subclass_map: Dict[str, List[str]] = {}
    interface_impl_map: Dict[str, List[str]] = {}
    
    method_callers_map: Dict[str, List[str]] = {}
    class_instantiators_map: Dict[str, List[str]] = {}

    for cls in public_classes:
        cls_name = cls.get("name", "")
        for base in cls.get("base_classes") or []:
            subclass_map.setdefault(base, []).append(cls_name)
        for interface in cls.get("implemented_interfaces") or []:
            interface_impl_map.setdefault(interface, []).append(cls_name)
            
    for func in public_functions:
        ref = symbol_ref(func)
        for call in func.get("calls") or []:
            method_callers_map.setdefault(call, []).append(ref)
        for inst in func.get("instantiations") or []:
            class_instantiators_map.setdefault(inst, []).append(ref)

    for func in public_functions:
        ref = symbol_ref(func)
        params = format_params(func.get("parameters", []))
        return_type = func.get("return_type") or ""
        file_path = func.get("file_path") or ""
        sig = _complete_signature(func.get("signature") or "", params, return_type)

        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="symbol_existence",
                question=f"Does function/method {ref} exist in version {ctx.version_tag or ctx.slice_id}?",
                answer="Yes",
                slice_id=ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="function_signature",
                question=f"What is the signature of function/method {ref}?",
                answer=sig or params,
                slice_id=ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="function_parameters",
                question=f"What are the parameters of function/method {ref}?",
                answer=params,
                slice_id=ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": file_path},
            )
        )
        if return_type:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="function_return_type",
                    question=f"What is the return type of function/method {ref}?",
                    answer=return_type,
                    slice_id=ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": file_path},
                )
            )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="symbol_location",
                question=f"Which file is function/method {ref} located in?",
                answer=file_path,
                slice_id=ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": file_path},
            )
        )
        
        calls = func.get("calls") or []
        instantiations = func.get("instantiations") or []
        field_accesses = func.get("field_accesses") or []
        string_literals = func.get("string_literals") or []
        
        if calls:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="symbol_callers",
                    question=f"What other functions or methods does function/method {ref} call internally?",
                    answer=", ".join(calls),
                    slice_id=ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": file_path},
                )
            )
        
        callers = method_callers_map.get(ref, [])
        if callers:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="symbol_call_sites",
                    question=f"What functions or methods call {ref} internally?",
                    answer=", ".join(set(callers)),
                    slice_id=ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": file_path},
                )
            )

        if instantiations:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="object_instantiations",
                    question=f"What classes/objects does function/method {ref} instantiate internally?",
                    answer=", ".join(instantiations),
                    slice_id=ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": file_path},
                )
            )

        if field_accesses:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="field_accesses",
                    question=f"What class fields/attributes does function/method {ref} access or manipulate internally?",
                    answer=", ".join(field_accesses),
                    slice_id=ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": file_path},
                )
            )

    for cls in public_classes:
        name = cls.get("name", "")
        base_classes = cls.get("base_classes") or []
        methods = cls.get("methods") or []
        fields = cls.get("fields") or []
        interfaces = cls.get("implemented_interfaces") or []
        file_path = cls.get("file_path") or ""

        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_existence",
                question=f"Does class {name} exist in version {ctx.version_tag or ctx.slice_id}?",
                answer="Yes",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_inheritance",
                question=f"What base classes does class {name} inherit from?",
                answer=", ".join(base_classes) if base_classes else "none",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_subclasses",
                question=f"What subclasses does class {name} have?",
                answer=", ".join(subclass_map.get(name, [])) if subclass_map.get(name) else "none",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_implemented_interfaces",
                question=f"What interfaces does class {name} implement?",
                answer=", ".join(interfaces) if interfaces else "none",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        for interface in interfaces:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="interface_implementors",
                    question=f"What classes implement interface {interface}?",
                    answer=", ".join(interface_impl_map.get(interface, [])) if interface_impl_map.get(interface) else "none",
                    slice_id=ctx.slice_id,
                    evidence={"kind": "interface", "name": interface, "file_path": file_path},
                )
            )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_methods",
                question=f"What methods does class {name} have?",
                answer=", ".join(methods) if methods else "none",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_fields",
                question=f"What fields/attributes does class {name} have?",
                answer=", ".join(f.get("name", "") for f in fields) if fields else "none",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )
        
        instantiators = class_instantiators_map.get(name, [])
        if instantiators:
            qas.append(
                make_qa(
                    repo=ctx.repo,
                    qa_type="intrinsic",
                    subtype="class_instantiation_sites",
                    question=f"Which functions/methods instantiate class {name} internally?",
                    answer=", ".join(set(instantiators)),
                    slice_id=ctx.slice_id,
                    evidence={"kind": "class", "name": name, "file_path": file_path},
                )
            )
            
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="symbol_location",
                question=f"Which file is class {name} located in?",
                answer=file_path,
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": file_path},
            )
        )

    version_label = ctx.version_tag or ctx.slice_id
    for neg_ref in (neg_func_refs or []):
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="symbol_existence",
                question=f"Does function/method {neg_ref} exist in version {version_label}?",
                answer="No",
                slice_id=ctx.slice_id,
                evidence={"kind": "function", "name": neg_ref, "file_path": ""},
            )
        )
    for neg_name in (neg_class_names or []):
        qas.append(
            make_qa(
                repo=ctx.repo,
                qa_type="intrinsic",
                subtype="class_existence",
                question=f"Does class {neg_name} exist in version {version_label}?",
                answer="No",
                slice_id=ctx.slice_id,
                evidence={"kind": "class", "name": neg_name, "file_path": ""},
            )
        )

    return qas

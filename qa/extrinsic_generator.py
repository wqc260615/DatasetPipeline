"""Generate CodeQA-style extrinsic QA pairs from docstrings."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

from qa.qa_common import make_qa, public_class, public_function, symbol_ref
from qa.qa_types import SliceContext

logger = logging.getLogger(__name__)

_NLP: Any = None


def _get_nlp() -> Any:
    global _NLP
    if _NLP is not None:
        return _NLP
    try:
        import spacy  # type: ignore
        _NLP = spacy.load("en_core_web_sm")
    except OSError:
        try:
            import spacy
            from spacy.cli import download  # type: ignore
            download("en_core_web_sm")
            _NLP = spacy.load("en_core_web_sm")
        except Exception as exc:
            logger.warning(
                "spaCy model 'en_core_web_sm' unavailable (%s). "
                "Run: python -m spacy download en_core_web_sm",
                exc,
            )
    except ImportError:
        logger.warning(
            "spaCy not installed; extrinsic QA generation will be skipped. "
            "Run: pip install spacy"
        )
    return _NLP


_TEMPORAL_PREPS: FrozenSet[str] = frozenset({
    "after", "before", "when", "while", "during", "once", "until",
    "upon", "whenever", "following",
})
_LOCATIVE_PREPS: FrozenSet[str] = frozenset({
    "at", "in", "on", "into", "onto", "near", "beside", "within",
    "among", "along", "across", "inside", "outside", "between",
    "above", "below", "over", "under", "behind",
})
# "using" and "via" are unambiguous manner markers; "by" is included since
# it most commonly expresses manner in technical docstrings.
_MANNER_PREPS: FrozenSet[str] = frozenset({"using", "via", "through", "by"})
_CAUSE_CONJ: FrozenSet[str] = frozenset({"since", "because"})

# Pronoun-only answers are not informative (CodeQA filters these out).
_PRONOUN_ANSWERS: FrozenSet[str] = frozenset({
    "it", "its", "they", "their", "them", "this", "that", "these",
    "those", "he", "she", "we", "you", "i",
})

# Generic subject phrases added by normalisation (not informative as answers).
_GENERIC_SUBJECTS: FrozenSet[str] = frozenset({
    "the function", "the method", "the code", "this function",
    "this method", "this code", "the class", "this class", "it",
})


def _classify_prep(prep_lower: str) -> Optional[str]:
    """Return the CodeQA semantic role for a preposition, or None."""
    if prep_lower in _TEMPORAL_PREPS:
        return "temporal"
    if prep_lower in _LOCATIVE_PREPS:
        return "locative"
    if prep_lower in _MANNER_PREPS:
        return "manner"
    return None


def _classify_advcl(token: Any) -> Optional[str]:
    """Return the CodeQA semantic role for an adverbial-clause token.

    Checks both ``mark`` children (formal subordinating conjunction) and
    ``advmod`` children (spaCy sometimes labels 'when'/'while' as advmod).
    Purpose is detected when the clause has a to-infinitive auxiliary but
    no mark (i.e., "to do X" rather than "in order to do X").
    """
    temporal_words = _TEMPORAL_PREPS | {"when", "while"}

    for child in token.children:
        if child.dep_ == "mark":
            word = child.text.lower()
            if word in temporal_words:
                return "temporal"
            if word in _CAUSE_CONJ:
                return "cause"
        elif child.dep_ == "advmod" and child.text.lower() in temporal_words:
            return "temporal"

    # to-infinitive: aux child "to" (POS=PART) without a mark sibling.
    has_mark = any(c.dep_ == "mark" for c in token.children)
    has_to_inf = any(
        c.dep_ == "aux" and c.text.lower() == "to" and c.pos_ == "PART"
        for c in token.children
    )
    if has_to_inf and not has_mark:
        return "purpose"

    return None


def _subtree_text(token: Any) -> str:
    """Return the full text of a token's dependency subtree."""
    tokens = sorted(token.subtree, key=lambda t: t.i)
    return "".join(t.text_with_ws for t in tokens).strip()


def _subtree_text_excluding(token: Any, excluded: Set[int]) -> str:
    """Return the subtree text of *token*, skipping any token whose index is
    in *excluded*."""
    tokens = [t for t in sorted(token.subtree, key=lambda t: t.i)
              if t.i not in excluded]
    return "".join(t.text_with_ws for t in tokens).strip()


def _aux_and_lemma(root: Any) -> Tuple[str, str]:
    """Return *(auxiliary_string, verb_lemma)* for interrogative inversion.

    Rules (following standard English grammar and CodeQA practice):
    - Explicit auxiliaries present → use them verbatim (inversion).
    - Root lemma is "be"           → use the inflected form as auxiliary.
    - Root lemma is "have"         → "has" (3rd-person singular present).
    - Otherwise                    → do-support with "does".
    """
    aux_tokens = sorted(
        (c for c in root.children if c.dep_ in ("aux", "auxpass")),
        key=lambda t: t.i,
    )
    if aux_tokens:
        aux_str = " ".join(t.text for t in aux_tokens)
        # Passive constructions keep the past participle (e.g. "associated");
        # active constructions use the infinitive so do-support provides tense.
        has_auxpass = any(t.dep_ == "auxpass" for t in aux_tokens)
        verb_form = root.text if has_auxpass else root.lemma_
        return aux_str, verb_form
    if root.lemma_ == "be":
        return root.text, "be"
    if root.lemma_ == "have":
        return "has", "have"
    return "does", root.lemma_


def _split_aux(aux_str: str) -> Tuple[str, str]:
    """Split a compound auxiliary string into (first_token, remainder).

    For interrogatives, the first auxiliary token moves before the subject
    while any remaining tokens (e.g. the copular 'be' in 'will be') stay
    between the subject and the main verb::

        'will be'  →  ('will', 'be')   → "When will [subj] be [verb]?"
        'does'     →  ('does', '')     → "When does [subj] [verb]?"
        'is'       →  ('is',   '')     → "What is [subj] [verb]?"
    """
    parts = aux_str.split(None, 1)
    if not parts:
        return "does", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _assemble_question(*parts: str) -> str:
    """Join non-empty *parts*, collapse whitespace, capitalise, and append '?'."""
    q = " ".join(p.strip() for p in parts if p.strip())
    q = re.sub(r"\s{2,}", " ", q).strip()
    if not q:
        return ""
    return q[0].upper() + q[1:] + "?"


def _build_qas_from_sentence(
    sent: Any,
    symbol_ref_str: str,
    symbol_kind: str,
    repo: str,
    slice_id: str,
    file_path: str,
) -> List[Dict[str, Any]]:
    """Generate all applicable QA pairs for a single parsed spaCy sentence."""
    qas: List[Dict[str, Any]] = []

    roots = [t for t in sent if t.dep_ == "ROOT"]
    if not roots:
        return qas
    root = roots[0]
    if root.pos_ not in ("VERB", "AUX"):
        return qas

    nsubj = next(
        (c for c in root.children if c.dep_ in ("nsubj", "nsubjpass")), None
    )
    if nsubj is None:
        return qas
    subj_text = _subtree_text(nsubj)

    obj = next((c for c in root.children if c.dep_ in ("dobj", "obj")), None)

    aux, verb_lemma = _aux_and_lemma(root)
    aux_first, aux_rest = _split_aux(aux)
    negated = any(c.dep_ == "neg" for c in root.children)

    if subj_text:
        subj_text = subj_text[0].lower() + subj_text[1:]

    structural_deps = {
        "nsubj", "nsubjpass", "dobj", "obj",
        "aux", "auxpass", "neg", "punct", "cc", "conj",
    }
    direct_mods: List[Any] = [c for c in root.children
                              if c.dep_ not in structural_deps]

    temporal: List[Any] = []
    locative: List[Any] = []
    manner: List[Any] = []
    cause: List[Any] = []
    purpose: List[Any] = []
    xcomp_list: List[Any] = []

    for mod in direct_mods:
        if mod.dep_ == "prep":
            role = _classify_prep(mod.text.lower())
            if role == "temporal":
                temporal.append(mod)
            elif role == "locative":
                locative.append(mod)
            elif role == "manner":
                manner.append(mod)
        elif mod.dep_ == "advcl":
            role = _classify_advcl(mod)
            if role == "temporal":
                temporal.append(mod)
            elif role == "cause":
                cause.append(mod)
            elif role == "purpose":
                purpose.append(mod)
        elif mod.dep_ == "xcomp":
            xcomp_list.append(mod)

    # Also look for manner acl/advcl nested anywhere inside the object subtree.
    # e.g. "creates an instance of a class using the specified classloader"
    # where "using" is dep=acl on "class", which is 3 levels into the obj subtree.
    if obj is not None:
        for tok in obj.subtree:
            if tok.i == obj.i:
                continue
            if tok.dep_ == "acl" and tok.pos_ == "VERB":
                if tok.lemma_ in ("use",) or tok.text.lower() == "using":
                    manner.append(tok)
            elif tok.dep_ == "prep" and tok.text.lower() in _MANNER_PREPS:
                # Exclude locative preps that happen to also appear in _MANNER_PREPS.
                if tok.text.lower() not in _LOCATIVE_PREPS:
                    manner.append(tok)

    evidence = {
        "kind": symbol_kind,
        "name": symbol_ref_str,
        "file_path": file_path,
    }

    def _qa(subtype: str, question: str, answer: str) -> Dict[str, Any]:
        if not question or not answer:
            return {}
        # Replace generic subject placeholders with the actual symbol name so
        # questions are self-contained (e.g. "the function" → "AnsiToWin32.write").
        question = re.sub(
            r'\b(?:the|this)\s+(?:function|method|class|code)\b',
            symbol_ref_str,
            question,
            flags=re.IGNORECASE,
        )
        if _RE_MULTILINE_NOISE.search(question) or _RE_MULTILINE_NOISE.search(answer):
            return {}
        if "{" in question or "{" in answer:
            return {}
        if len(question.split()) < 4:
            return {}
        if subtype != "yesno" and len(answer.split()) < 3:
            return {}
        return make_qa(
            repo=repo,
            qa_type="extrinsic",
            subtype=subtype,
            question=question,
            answer=answer,
            slice_id=slice_id,
            evidence=evidence,
        )

    obj_indices: Set[int] = {t.i for t in obj.subtree} if obj else set()

    def _other_mods(*exclude: Any) -> str:
        exclude_set = {m.i for m in exclude}
        parts = []
        for m in sorted(direct_mods, key=lambda t: t.i):
            if m.i in exclude_set:
                continue
            if m.i in obj_indices:
                continue
            parts.append(_subtree_text(m))
        return " ".join(parts)

    if obj is not None:
        obj_text = _subtree_text(obj)
        if obj_text.lower() not in _PRONOUN_ANSWERS:
            extra = _other_mods()
            q = _assemble_question(
                "What", aux_first, subj_text, aux_rest, verb_lemma, extra
            )
            r = _qa("wh_object", q, obj_text)
            if r:
                qas.append(r)

    for xc in xcomp_list:
        xcomp_text = _subtree_text(xc)
        if xcomp_text.lower() not in _PRONOUN_ANSWERS:
            # Keep conditional advcl modifiers ("if …", "when …") in question.
            cond_parts = [
                _subtree_text(m) for m in direct_mods
                if m.dep_ == "advcl" and _classify_advcl(m) is None
            ]
            q = _assemble_question(
                "What", aux_first, subj_text, aux_rest, verb_lemma, *cond_parts
            )
            r = _qa("wh_xcomp", q, xcomp_text)
            if r:
                qas.append(r)

    # For subject questions there is NO subject-auxiliary inversion; the
    # auxiliary and verb keep their original declarative order.  Only
    # generated for passive subjects (nsubjpass dep or auxpass present) to
    # avoid broken active-voice constructions like "What saved?".
    _is_passive_subj = (
        nsubj.dep_ == "nsubjpass"
        or any(c.dep_ == "auxpass" for c in root.children)
    )
    if _is_passive_subj and subj_text.lower() not in _GENERIC_SUBJECTS:
        # Resolve pronoun subjects ("this", "it", …) to the symbol name so the
        # answer is informative rather than a meaningless referential void.
        answer_subj = (
            symbol_ref_str if subj_text.lower() in _PRONOUN_ANSWERS else subj_text
        )
        # Skip self-referential answers where the answer IS the symbol being
        # queried (the question already names it; ROUGE-L score is meaningless).
        if answer_subj.lower() == symbol_ref_str.lower():
            answer_subj = ""
        # Build VP: all tokens in the sentence except subject subtree + punctuation.
        # Keep aux tokens in their original position (no inversion for subject-Qs).
        subj_indices: Set[int] = {t.i for t in nsubj.subtree}
        vp_tokens = []
        for t in sorted(sent, key=lambda t: t.i):
            if t.i in subj_indices or t.dep_ == "punct":
                continue
            vp_tokens.append(t.text_with_ws.rstrip())
        vp_text = " ".join(p for p in vp_tokens if p)
        q = _assemble_question("What", vp_text)
        # Anchor to entity for self-containment: prevents questions like
        # "What should be wrapped?" that are ambiguous without context.
        if q.endswith("?"):
            q = q[:-1] + f", according to {symbol_ref_str}?"
        if answer_subj:
            r = _qa("wh_subject", q, answer_subj)
            if r:
                qas.append(r)

    # Question keeps obj (core, minus nested manner mod) and other mods.
    def _wh_adjunct(wh_word: str, target_mod: Any) -> str:
        if obj is not None:
            # If a manner modifier is nested inside the obj subtree, exclude it
            # so the question reads "How does X create an instance" not
            # "How does X create an instance using the classloader".
            nested_in_obj = target_mod.i in obj_indices
            if nested_in_obj and wh_word == "How":
                excl = {t.i for t in target_mod.subtree}
                obj_core = _subtree_text_excluding(obj, excl)
            else:
                obj_core = _subtree_text(obj)
        else:
            obj_core = ""
        other = _other_mods(target_mod)
        return _assemble_question(
            wh_word, aux_first, subj_text, aux_rest, verb_lemma, obj_core, other
        )

    for mod in temporal:
        ans = _subtree_text(mod)
        q = _wh_adjunct("When", mod)
        r = _qa("wh_temporal", q, ans)
        if r:
            qas.append(r)

    for mod in locative:
        ans = _subtree_text(mod)
        q = _wh_adjunct("Where", mod)
        r = _qa("wh_locative", q, ans)
        if r:
            qas.append(r)

    for mod in manner:
        ans = _subtree_text(mod)
        q = _wh_adjunct("How", mod)
        r = _qa("wh_manner", q, ans)
        if r:
            qas.append(r)

    for mod in cause:
        ans = _subtree_text(mod)
        q = _wh_adjunct("Why", mod)
        r = _qa("wh_cause", q, ans)
        if r:
            qas.append(r)

    for mod in purpose:
        ans = _subtree_text(mod)
        q = _assemble_question(
            "For what purpose", aux_first, subj_text, aux_rest, verb_lemma,
            _subtree_text(obj) if obj else "", _other_mods(mod),
        )
        r = _qa("wh_purpose", q, ans)
        if r:
            qas.append(r)

    # Only emit yes/no when the sentence also has adjunct modifiers so that
    # yes/no pairs co-occur with richer WH questions.  This keeps their
    # proportion low relative to WH-type questions (50 % random baseline
    # makes standalone yes/no questions a weak evaluation signal).
    has_adjuncts = bool(temporal or locative or manner or cause or purpose)
    if obj is not None and has_adjuncts:
        obj_text_yn = _subtree_text(obj)
        other_yn = _other_mods()
        q = _assemble_question(
            aux_first.capitalize(), subj_text, aux_rest,
            verb_lemma, obj_text_yn, other_yn
        )
        r = _qa("yesno", q, "No" if negated else "Yes")
        if r:
            qas.append(r)

    return qas


_RE_QUOTE_STRIP = re.compile(r'^[\s"\' ]+|[\s"\' ]+$')
_RE_BLANK_LINE = re.compile(r'\n\s*\n')
_RE_CODE_EXAMPLE = re.compile(r'^\s*>>>.*$', re.MULTILINE)
_RE_HTML_TAG = re.compile(r'<[a-zA-Z][^>]*>', re.IGNORECASE)
_RE_TMPL_VAR = re.compile(r'\{[a-zA-Z_]\w*\}')
_RE_MULTILINE_NOISE = re.compile(r'\n|<[a-zA-Z/]')

def _clean_docstring(text: str) -> str:
    """Strip quotes/whitespace; return only the first paragraph.

    Returns an empty string if the content appears to be HTML, an f-string
    template, or otherwise non-prose (these produce gibberish questions).
    """
    text = _RE_QUOTE_STRIP.sub("", text)
    text = _RE_CODE_EXAMPLE.sub("", text).strip()
    if _RE_HTML_TAG.search(text):
        return ""
    if len(_RE_TMPL_VAR.findall(text)) >= 3:
        return ""
    if re.match(r'^f["\']', text.lstrip()):
        return ""
    return _RE_BLANK_LINE.split(text)[0].strip()


def _sentence_text(sent: Any) -> str:
    return "".join(t.text_with_ws for t in sent).strip()


def _needs_subject(sent: Any) -> bool:
    """Return True if the sentence root has no explicit subject."""
    root = next((t for t in sent if t.dep_ == "ROOT"), None)
    if root is None or root.pos_ not in ("VERB", "AUX"):
        return False
    return not any(c.dep_ in ("nsubj", "nsubjpass") for c in root.children)


def _docstring_to_qas(
    docstring: str,
    symbol_ref_str: str,
    symbol_kind: str,
    repo: str,
    slice_id: str,
    file_path: str,
) -> List[Dict[str, Any]]:
    """Parse *docstring* and generate all applicable QA pairs."""
    nlp = _get_nlp()
    if nlp is None:
        return []

    text = _clean_docstring(docstring)
    if not text or len(text) < 15:
        return []

    qas: List[Dict[str, Any]] = []

    doc = nlp(text)
    for sent in doc.sents:
        sent_str = _sentence_text(sent)

        if _needs_subject(sent):
            normalized = f"The {symbol_kind} {sent_str[0].lower()}{sent_str[1:]}"
            doc2 = nlp(normalized)
            for s2 in doc2.sents:
                qas.extend(
                    _build_qas_from_sentence(
                        s2, symbol_ref_str, symbol_kind,
                        repo, slice_id, file_path,
                    )
                )
        else:
            qas.extend(
                _build_qas_from_sentence(
                    sent, symbol_ref_str, symbol_kind,
                    repo, slice_id, file_path,
                )
            )

    return qas


def build_extrinsic_qas(ctx: SliceContext) -> List[Dict[str, Any]]:
    """Generate extrinsic (docstring-derived) QA pairs for one slice context.

    Iterates over every public function and class in *ctx*.  For each symbol
    whose ``doc`` field contains a non-trivial description, runs the
    CodeQA-style template pipeline to produce wh-/yes-no question pairs.
    """
    qas: List[Dict[str, Any]] = []

    for func in ctx.functions:
        if not public_function(func):
            continue
        docstring = func.get("doc") or ""
        if not docstring:
            continue
        ref = symbol_ref(func)
        qas.extend(
            _docstring_to_qas(
                docstring=docstring,
                symbol_ref_str=ref,
                symbol_kind="function",
                repo=ctx.repo,
                slice_id=ctx.slice_id,
                file_path=func.get("file_path") or "",
            )
        )

    for cls in ctx.classes:
        if not public_class(cls):
            continue
        docstring = cls.get("doc") or ""
        if not docstring:
            continue
        qas.extend(
            _docstring_to_qas(
                docstring=docstring,
                symbol_ref_str=cls.get("name") or "",
                symbol_kind="class",
                repo=ctx.repo,
                slice_id=ctx.slice_id,
                file_path=cls.get("file_path") or "",
            )
        )

    return qas

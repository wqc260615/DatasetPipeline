"""Generate question-answer pairs from repository evolution data.

This module creates temporally grounded Q&A pairs for the benchmark, covering:
- RQ1: Factual questions about repository state at specific versions
- RQ2: Questions about code evolution events (rename, move, refactor)
- RQ3: Questions designed to detect temporal leakage
- RQ4: Multi-version reasoning questions

Each Q&A pair includes:
- The question in natural language (as a developer would actually ask)
- The ground-truth answer
- Metadata (commit, timestamp, question type, context requirements)
"""

import argparse
import json
import random
import re
from pathlib import Path
from typing import Dict, List, Optional


# =============================================================================
# Entity Filtering - Exclude low-value entities
# =============================================================================

# Patterns for entities that are not valuable for QA
LOW_VALUE_PATTERNS = [
    r"^test_",           # Test functions
    r"^Test",            # Test classes (prefix)
    r"Test$",            # Test classes (suffix)
    r"TestCase$",        # TestCase classes
    r"TestSuite$",       # TestSuite classes
    r"_test$",           # Suffix test
    r"^__.*__$",         # Dunder methods
    r"^setUp$",          # Test setup
    r"^tearDown$",       # Test teardown
    r"^main$",           # Main functions
    r"^_[^_]",           # Private methods (single underscore)
    r"^publish$",        # Common setup.py function
]

# Files to exclude
LOW_VALUE_FILES = [
    "test_",
    "tests/",
    "setup.py",
    "conftest.py",
    "__init__.py",
    "_test.py",
]


def _is_valuable_entity(entity_key: str, entity_info: Dict) -> bool:
    """Check if an entity is valuable enough to generate questions about."""
    name = entity_info.get("name", "")
    
    # Get file path - could be in 'file' (single) or 'files' (list)
    file_path = entity_info.get("file", "")
    if not file_path:
        files = entity_info.get("files", [])
        file_path = files[0] if files else ""
    
    # Also check the entity key which contains the file path
    entity_file = entity_key.split("::")[1] if "::" in entity_key else ""
    
    # Check name patterns
    for pattern in LOW_VALUE_PATTERNS:
        if re.match(pattern, name):
            return False
    
    # Check file patterns against both sources
    for file_pattern in LOW_VALUE_FILES:
        if file_path and file_pattern in file_path:
            return False
        if entity_file and file_pattern in entity_file:
            return False
    
    return True


def _get_valuable_entities(entity_index: Dict) -> Dict[str, Dict]:
    """Filter entity index to only include valuable entities."""
    return {
        key: info for key, info in entity_index.items()
        if _is_valuable_entity(key, info)
    }


# =============================================================================
# RQ1 Templates - Factual questions about repository state
# =============================================================================

RQ1_TEMPLATES = {
    "availability": [
        {
            "template": (
                "I am working with the codebase at this point in its history. "
                "Is there already support for `{name}`, or was it added later?"
            ),
            "answer_template": "{availability_explanation}",
            "type": "factual",
        },
        {
            "template": (
                "I'm looking at the project as of {version}. Does the API include "
                "a way to {concept_description}?"
            ),
            "answer_template": "{availability_explanation}",
            "type": "factual",
        },
    ],
    "introduction_time": [
        {
            "template": "When did `{name}` first become part of the project?",
            "answer_template": "{commit_or_version}",
            "type": "factual",
        },
        {
            "template": "In which release was `{name}` introduced to the codebase?",
            "answer_template": "{commit_or_version}",
            "type": "factual",
        },
        {
            "template": (
                "I need to know when `{name}` became available. "
                "What version should I target for minimum compatibility?"
            ),
            "answer_template": "{commit_or_version}",
            "type": "factual",
        },
    ],
    "usage_scope": [
        {
            "template": (
                "At this stage of the project, which components provide "
                "functionality related to `{concept}`?"
            ),
            "answer_template": "{entity_list}",
            "type": "list",
        },
        {
            "template": (
                "In {version}, what functions are available in the `{module}` module?"
            ),
            "answer_template": "{entity_list}",
            "type": "list",
        },
    ],
    "api_shape": [
        {
            "template": "How should `{name}` be called at this point in the project?",
            "answer_template": "{signature}",
            "type": "factual",
        },
        {
            "template": "What parameters does `{name}` accept in {version}?",
            "answer_template": "{params}",
            "type": "factual",
        },
        {
            "template": (
                "I want to use `{name}` in my code. What's the correct way to "
                "invoke it at this point in the project's history?"
            ),
            "answer_template": "{signature}",
            "type": "factual",
        },
    ],
    "location_discovery": [
        {
            "template": "Where is `{name}` implemented in the codebase at this time?",
            "answer_template": "{file}",
            "type": "factual",
        },
        {
            "template": (
                "I need to look at the implementation of `{name}`. "
                "Which file should I check in {version}?"
            ),
            "answer_template": "{file}",
            "type": "factual",
        },
    ],
}


# =============================================================================
# RQ2 Templates - Code evolution event questions
# =============================================================================

RQ2_TEMPLATES = {
    "migration_confusion": [
        {
            "template": (
                "My code used to rely on `{old_name}`, but it no longer works "
                "after this update. What should I use instead?"
            ),
            "answer_template": "{new_name_explanation}",
            "type": "change_reasoning",
        },
        {
            "template": (
                "I was using `{old_name}` in my application, but after upgrading "
                "the library I get an error. Has it been renamed or replaced?"
            ),
            "answer_template": "{new_name_explanation}",
            "type": "change_reasoning",
        },
    ],
    "location_change": [
        {
            "template": (
                "I can no longer find `{name}` where it used to be. "
                "Has its implementation been relocated?"
            ),
            "answer_template": "{old_new_location}",
            "type": "change_reasoning",
        },
        {
            "template": (
                "The import path for `{name}` seems to have changed. "
                "Where should I import it from now?"
            ),
            "answer_template": "{old_new_location}",
            "type": "change_reasoning",
        },
    ],
    "signature_breakage": [
        {
            "template": "Why does calling `{name}` fail after this update?",
            "answer_template": "{signature_change_explanation}",
            "type": "change_reasoning",
        },
        {
            "template": (
                "I'm getting a TypeError when calling `{name}` after updating. "
                "What changed in its signature?"
            ),
            "answer_template": "{signature_change_explanation}",
            "type": "change_reasoning",
        },
        {
            "template": (
                "My call to `{name}({old_args})` stopped working. "
                "How should I update my code?"
            ),
            "answer_template": "{signature_change_explanation}",
            "type": "change_reasoning",
        },
    ],
    "before_after_behavior": [
        {
            "template": (
                "How did `{name}` behave before this change, and how does it "
                "behave now?"
            ),
            "answer_template": "{before_after_summary}",
            "type": "comparison",
        },
        {
            "template": (
                "What's different about `{name}` between {old_version} and "
                "{new_version}?"
            ),
            "answer_template": "{before_after_summary}",
            "type": "comparison",
        },
    ],
}


# =============================================================================
# RQ3 Templates - Temporal leakage detection
# =============================================================================

RQ3_TEMPLATES = {
    "historical_behavior": [
        {
            "template": "At this point in the project, how does `{name}` behave?",
            "answer_template": "{behavior_description}",
            "type": "factual",
        },
        {
            "template": (
                "I'm debugging code written against {version}. "
                "What does `{name}` return for typical inputs?"
            ),
            "answer_template": "{behavior_description}",
            "type": "factual",
        },
    ],
    "pre_feature_boundary": [
        {
            "template": (
                "Before `{feature}` was introduced, how was `{related_task}` handled?"
            ),
            "answer_template": "{pre_feature_behavior}",
            "type": "factual",
        },
        {
            "template": (
                "In {version}, what approach was used for `{related_task}` "
                "(before the current implementation)?"
            ),
            "answer_template": "{pre_feature_behavior}",
            "type": "factual",
        },
    ],
    "assumption_check": [
        {
            "template": (
                "Is it safe to assume that `{name}` already supports `{feature}` "
                "at this point?"
            ),
            "answer_template": "{yes_no_explanation}",
            "type": "analysis",
        },
        {
            "template": (
                "Can I rely on `{name}` having `{capability}` in {version}?"
            ),
            "answer_template": "{yes_no_explanation}",
            "type": "analysis",
        },
        {
            "template": (
                "I'm writing code for {version}. Does `{name}` exist yet, "
                "or do I need to implement this myself?"
            ),
            "answer_template": "{yes_no_explanation}",
            "type": "analysis",
        },
    ],
}


# =============================================================================
# RQ4 Templates - Multi-version reasoning
# =============================================================================

RQ4_TEMPLATES = {
    "evolution_overview": [
        {
            "template": "How has `{name}` evolved over the lifetime of the project?",
            "answer_template": "{evolution_summary}",
            "type": "summary",
        },
        {
            "template": (
                "Can you trace the history of `{name}` from when it was first "
                "added to its current form?"
            ),
            "answer_template": "{evolution_summary}",
            "type": "summary",
        },
        {
            "template": (
                "What major changes has `{name}` gone through since it was introduced?"
            ),
            "answer_template": "{evolution_summary}",
            "type": "summary",
        },
    ],
    "regression_analysis": [
        {
            "template": (
                "When did `{name}` stop behaving as expected, and what caused it?"
            ),
            "answer_template": "{regression_explanation}",
            "type": "analysis",
        },
        {
            "template": (
                "Something broke with `{name}` at some point. "
                "Can you identify which commit introduced the problem?"
            ),
            "answer_template": "{regression_explanation}",
            "type": "analysis",
        },
    ],
    "compatibility_decision": [
        {
            "template": (
                "Would code written for earlier releases still work with the "
                "current version of `{name}`?"
            ),
            "answer_template": "{compatibility_analysis}",
            "type": "analysis",
        },
        {
            "template": (
                "Is `{name}` backward compatible? Can I upgrade without changing "
                "my existing code that uses it?"
            ),
            "answer_template": "{compatibility_analysis}",
            "type": "analysis",
        },
        {
            "template": (
                "If I wrote code using `{name}` in {old_version}, will it still "
                "work in {new_version}?"
            ),
            "answer_template": "{compatibility_analysis}",
            "type": "analysis",
        },
    ],
    "change_localization": [
        {
            "template": (
                "During which period did the most significant changes to "
                "`{name}` occur?"
            ),
            "answer_template": "{version_range}",
            "type": "factual",
        },
        {
            "template": (
                "When was `{name}` most actively developed? "
                "Which versions saw the biggest changes?"
            ),
            "answer_template": "{version_range}",
            "type": "factual",
        },
    ],
}


# =============================================================================
# Helper Functions
# =============================================================================

def _format_version(commit_hash: str, tags: List[str]) -> str:
    """Format version string preferring tag names over commit hashes."""
    if tags:
        return tags[0]
    return f"commit {commit_hash[:8]}"


def _format_version_for_context(commit_hash: str, tags: List[str]) -> str:
    """Format version for context setting in questions."""
    if tags:
        return f"version {tags[0]}"
    return f"commit {commit_hash[:8]}"


def _get_signature_from_event(event: Dict) -> Optional[str]:
    """Extract signature from an entity event."""
    return event.get("signature") or event.get("new_signature")


def _describe_signature(signature: str) -> str:
    """Convert signature to human-readable description."""
    if not signature:
        return "no specific signature available"
    return f"`{signature}`"


def _extract_module_from_file(file_path: str) -> str:
    """Extract module name from file path."""
    # Remove .py extension and convert path to module notation
    if file_path.endswith(".py"):
        file_path = file_path[:-3]
    return file_path.replace("/", ".")


def _infer_concept_from_name(name: str) -> str:
    """Infer a concept description from entity name."""
    # Convert camelCase or snake_case to readable text
    # e.g., "get_user_data" -> "getting user data"
    words = re.split(r'[_\s]+|(?<=[a-z])(?=[A-Z])', name)
    words = [w.lower() for w in words if w]
    
    if not words:
        return "this functionality"
    
    if words[0] in ['get', 'fetch', 'retrieve']:
        return f"retrieving {' '.join(words[1:])}" if len(words) > 1 else "getting data"
    elif words[0] in ['set', 'update', 'save']:
        return f"setting {' '.join(words[1:])}" if len(words) > 1 else "updating data"
    elif words[0] in ['create', 'make', 'build']:
        return f"creating {' '.join(words[1:])}" if len(words) > 1 else "creating objects"
    elif words[0] in ['delete', 'remove']:
        return f"removing {' '.join(words[1:])}" if len(words) > 1 else "removing data"
    elif words[0] in ['send', 'post']:
        return f"sending {' '.join(words[1:])}" if len(words) > 1 else "sending requests"
    elif words[0] == 'is' or words[0] == 'has':
        return f"checking if {' '.join(words[1:])}" if len(words) > 1 else "checking conditions"
    elif words[0] in ['encode', 'decode']:
        return f"{words[0]}ing {' '.join(words[1:])}" if len(words) > 1 else f"{words[0]}ing data"
    elif words[0] in ['parse', 'format']:
        return f"{words[0]}ing {' '.join(words[1:])}" if len(words) > 1 else f"{words[0]}ing content"
    elif words[0] in ['handle', 'process']:
        return f"handling {' '.join(words[1:])}" if len(words) > 1 else "handling operations"
    elif words[0] in ['validate', 'verify', 'check']:
        return f"validating {' '.join(words[1:])}" if len(words) > 1 else "validation"
    elif words[0] in ['auth', 'authentication', 'authorize']:
        return "authentication" if len(words) == 1 else f"handling {' '.join(words)} authentication"
    elif words[0] in ['error', 'exception']:
        return f"error handling for {' '.join(words[1:])}" if len(words) > 1 else "error handling"
    elif words[0] in ['request', 'response']:
        return f"HTTP {words[0]}s" if len(words) == 1 else f"handling {' '.join(words)}"
    elif len(words) == 1:
        # Single word - make it more descriptive
        return f"{words[0]} operations"
    else:
        return f"{' '.join(words)} functionality"


# =============================================================================
# Question Generation Functions
# =============================================================================

def _generate_rq1_questions(
    entity_timeline: Dict,
    version_entities: Dict[str, List[str]],
    parsed_data: Dict[str, Dict],
    commits: List[Dict],
    max_per_category: int = 15,
) -> List[Dict]:
    """Generate RQ1 questions about repository state at specific versions."""
    questions = []
    entity_index = _get_valuable_entities(entity_timeline.get("entity_index", {}))
    entity_events = entity_timeline.get("entity_events", [])
    
    if not entity_index:
        return questions
    
    # Build event lookup for signatures
    event_by_entity: Dict[str, List[Dict]] = {}
    for event in entity_events:
        key = event.get("entity_key")
        if key:
            event_by_entity.setdefault(key, []).append(event)
    
    # 1. Availability questions - ask about entities that exist in specific versions
    for commit in commits:
        commit_hash = commit["hash"]
        tags = commit.get("tags", [])
        version = _format_version(commit_hash, tags)
        version_ctx = _format_version_for_context(commit_hash, tags)
        
        entities_in_version = version_entities.get(commit_hash, [])
        valuable_in_version = [e for e in entities_in_version if e in entity_index]
        
        for entity_key in random.sample(valuable_in_version, min(2, len(valuable_in_version))):
            info = entity_index[entity_key]
            name = info["name"]
            entity_type = info["type"]
            
            template = random.choice(RQ1_TEMPLATES["availability"])
            question_text = template["template"].format(
                name=name,
                version=version_ctx,
                concept_description=_infer_concept_from_name(name),
            )
            
            questions.append({
                "rq": "RQ1",
                "category": "availability",
                "question": question_text,
                "answer": f"Yes, `{name}` is available at this point. It's a {entity_type} "
                         f"located in `{info.get('files', ['unknown'])[0]}`.",
                "ground_truth": {
                    "exists": True,
                    "entity_key": entity_key,
                    "entity_type": entity_type,
                    "file": info.get("files", ["unknown"])[0],
                },
                "commit": commit_hash,
                "timestamp": commit.get("timestamp"),
                "tags": tags,
                "metadata": {
                    "entity_type": entity_type,
                    "entity_name": name,
                    "question_type": template["type"],
                },
            })
        
        if len(questions) >= max_per_category:
            break
    
    # 2. Introduction time questions
    intro_count = 0
    for entity_key, info in entity_index.items():
        if intro_count >= max_per_category:
            break
        
        intro_commit = info.get("introduced_in")
        intro_tags = info.get("introduced_tags", [])
        intro_version = _format_version(intro_commit, intro_tags) if intro_commit else "unknown"
        
        template = random.choice(RQ1_TEMPLATES["introduction_time"])
        question_text = template["template"].format(
            name=info["name"],
        )
        
        answer_text = f"`{info['name']}` was first introduced in {intro_version}"
        if intro_tags:
            answer_text += f" (tag: {intro_tags[0]})"
        answer_text += f", on {info.get('introduced_at', 'unknown date')[:10]}."
        
        questions.append({
            "rq": "RQ1",
            "category": "introduction_time",
            "question": question_text,
            "answer": answer_text,
            "ground_truth": {
                "commit": intro_commit,
                "tags": intro_tags,
                "timestamp": info.get("introduced_at"),
            },
            "commit": intro_commit,
            "timestamp": info.get("introduced_at"),
            "tags": intro_tags,
            "entity_key": entity_key,
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": template["type"],
            },
        })
        intro_count += 1
    
    # 3. API shape questions (signature)
    sig_count = 0
    for entity_key, info in entity_index.items():
        if info["type"] != "function" or sig_count >= max_per_category:
            continue
        
        # Get signature from events
        events = event_by_entity.get(entity_key, [])
        signature = None
        for e in events:
            if e.get("event") == "introduced":
                signature = e.get("signature")
                break
        
        if not signature:
            continue
        
        intro_commit = info.get("introduced_in")
        intro_tags = info.get("introduced_tags", [])
        version_ctx = _format_version_for_context(intro_commit, intro_tags) if intro_commit else "this version"
        
        template = random.choice(RQ1_TEMPLATES["api_shape"])
        question_text = template["template"].format(
            name=info["name"],
            version=version_ctx,
            params=signature,
        )
        
        questions.append({
            "rq": "RQ1",
            "category": "api_shape",
            "question": question_text,
            "answer": f"The function should be called as `{signature}`.",
            "ground_truth": {
                "signature": signature,
            },
            "commit": intro_commit,
            "timestamp": info.get("introduced_at"),
            "tags": intro_tags,
            "entity_key": entity_key,
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": template["type"],
            },
        })
        sig_count += 1
    
    # 4. Location discovery questions
    loc_count = 0
    for entity_key, info in entity_index.items():
        if loc_count >= max_per_category:
            break
        
        files = info.get("files", [])
        if not files:
            continue
        
        intro_commit = info.get("introduced_in")
        intro_tags = info.get("introduced_tags", [])
        version_ctx = _format_version_for_context(intro_commit, intro_tags) if intro_commit else "this version"
        
        template = random.choice(RQ1_TEMPLATES["location_discovery"])
        question_text = template["template"].format(
            name=info["name"],
            version=version_ctx,
        )
        
        questions.append({
            "rq": "RQ1",
            "category": "location_discovery",
            "question": question_text,
            "answer": f"`{info['name']}` is implemented in `{files[0]}`.",
            "ground_truth": {
                "file": files[0],
                "all_files": files,
            },
            "commit": intro_commit,
            "timestamp": info.get("introduced_at"),
            "tags": intro_tags,
            "entity_key": entity_key,
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": template["type"],
            },
        })
        loc_count += 1
    
    # 5. Usage scope questions - list functions in a module
    scope_count = 0
    for commit in commits[:5]:  # Limit to first few commits
        commit_hash = commit["hash"]
        tags = commit.get("tags", [])
        version = _format_version(commit_hash, tags)
        
        parsed = parsed_data.get(commit_hash, {})
        files = parsed.get("files", {})
        
        for file_path, file_info in files.items():
            if scope_count >= max_per_category:
                break
            if "error" in file_info or any(p in file_path for p in LOW_VALUE_FILES):
                continue
            
            functions = [f["name"] for f in file_info.get("functions", [])
                        if not any(re.match(p, f["name"]) for p in LOW_VALUE_PATTERNS)]
            if len(functions) < 2:
                continue
            
            module = _extract_module_from_file(file_path)
            template = random.choice(RQ1_TEMPLATES["usage_scope"])
            question_text = template["template"].format(
                concept=module.split(".")[-1],
                version=version,
                module=module,
            )
            
            questions.append({
                "rq": "RQ1",
                "category": "usage_scope",
                "question": question_text,
                "answer": f"The following functions are available: {', '.join(f'`{f}`' for f in functions)}.",
                "ground_truth": {
                    "functions": functions,
                    "file": file_path,
                },
                "commit": commit_hash,
                "timestamp": commit.get("timestamp"),
                "tags": tags,
                "metadata": {
                    "file": file_path,
                    "question_type": template["type"],
                },
            })
            scope_count += 1
    
    return questions


def _generate_rq2_questions(
    change_history: Dict,
    entity_timeline: Dict,
    max_per_category: int = 15,
) -> List[Dict]:
    """Generate RQ2 questions about code evolution events."""
    questions = []
    entity_index = _get_valuable_entities(entity_timeline.get("entity_index", {}))
    
    for version_change in change_history.get("change_history", []):
        old_commit = version_change["old_commit"]
        new_commit = version_change["new_commit"]
        old_version = f"commit {old_commit[:8]}"
        new_version = f"commit {new_commit[:8]}"
        
        for change in version_change.get("changes", []):
            change_type = change.get("change_type")
            entity_key = change.get("entity_key") or change.get("old_key")
            
            # Skip low-value entities
            if entity_key and not any(entity_key.startswith(f"{t}::") and 
                                     _is_valuable_entity(entity_key, change) 
                                     for t in ["function", "class"]):
                name = change.get("name") or change.get("old_name")
                if any(re.match(p, name or "") for p in LOW_VALUE_PATTERNS):
                    continue
            
            if change_type == "renamed":
                old_name = change["old_name"]
                new_name = change["new_name"]
                
                template = random.choice(RQ2_TEMPLATES["migration_confusion"])
                question_text = template["template"].format(
                    old_name=old_name,
                )
                
                answer_text = (
                    f"`{old_name}` has been renamed to `{new_name}`. "
                    f"Update your code to use the new name."
                )
                
                questions.append({
                    "rq": "RQ2",
                    "category": "migration_confusion",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "change_type": "renamed",
                        "old_name": old_name,
                        "new_name": new_name,
                    },
                    "before_commit": old_commit,
                    "after_commit": new_commit,
                    "metadata": {
                        "entity_type": change.get("entity_type"),
                        "confidence": change.get("confidence"),
                        "question_type": template["type"],
                    },
                })
            
            elif change_type == "moved":
                name = change.get("old_name") or change.get("name")
                old_file = change["old_file"]
                new_file = change["new_file"]
                
                template = random.choice(RQ2_TEMPLATES["location_change"])
                question_text = template["template"].format(
                    name=name,
                )
                
                answer_text = (
                    f"Yes, `{name}` has been moved from `{old_file}` to `{new_file}`. "
                    f"Update your imports accordingly."
                )
                
                questions.append({
                    "rq": "RQ2",
                    "category": "location_change",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "change_type": "moved",
                        "old_file": old_file,
                        "new_file": new_file,
                    },
                    "before_commit": old_commit,
                    "after_commit": new_commit,
                    "metadata": {
                        "entity_type": change.get("entity_type"),
                        "question_type": template["type"],
                    },
                })
            
            elif change_type == "signature_changed":
                name = change.get("name")
                old_args = change.get("old_args", [])
                new_args = change.get("new_args", [])
                added_args = set(new_args) - set(old_args)
                removed_args = set(old_args) - set(new_args)
                
                if not (added_args or removed_args):
                    continue
                
                template = random.choice(RQ2_TEMPLATES["signature_breakage"])
                old_args_str = ", ".join(old_args) if old_args else "no arguments"
                question_text = template["template"].format(
                    name=name,
                    old_args=old_args_str,
                )
                
                changes_desc = []
                if added_args:
                    changes_desc.append(f"new parameters were added: {', '.join(added_args)}")
                if removed_args:
                    changes_desc.append(f"parameters were removed: {', '.join(removed_args)}")
                
                answer_text = (
                    f"The signature of `{name}` has changed. {' '.join(changes_desc).capitalize()}. "
                    f"The new signature uses: ({', '.join(new_args)})."
                )
                
                questions.append({
                    "rq": "RQ2",
                    "category": "signature_breakage",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "change_type": "signature_changed",
                        "old_args": old_args,
                        "new_args": new_args,
                        "added_args": list(added_args),
                        "removed_args": list(removed_args),
                    },
                    "before_commit": old_commit,
                    "after_commit": new_commit,
                    "metadata": {
                        "entity_name": name,
                        "question_type": template["type"],
                    },
                })
            
            elif change_type == "added":
                name = change.get("name")
                if any(re.match(p, name or "") for p in LOW_VALUE_PATTERNS):
                    continue
                
                file_path = change.get("file", "unknown")
                entity_type = change.get("entity_type", "entity")
                
                # Frame as before/after behavior question
                template = random.choice(RQ2_TEMPLATES["before_after_behavior"])
                question_text = template["template"].format(
                    name=name,
                    old_version=old_version,
                    new_version=new_version,
                )
                
                answer_text = (
                    f"`{name}` did not exist in {old_version}. "
                    f"It was introduced in {new_version} in `{file_path}`."
                )
                
                questions.append({
                    "rq": "RQ2",
                    "category": "before_after_behavior",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "change_type": "added",
                        "name": name,
                        "file": file_path,
                        "introduced_in": new_commit,
                    },
                    "before_commit": old_commit,
                    "after_commit": new_commit,
                    "metadata": {
                        "entity_type": entity_type,
                        "question_type": template["type"],
                    },
                })
            
            if len(questions) >= max_per_category * 4:
                break
        
        if len(questions) >= max_per_category * 4:
            break
    
    return questions


def _generate_rq3_questions(
    entity_timeline: Dict,
    version_entities: Dict[str, List[str]],
    commits: List[Dict],
    max_per_category: int = 15,
) -> List[Dict]:
    """Generate RQ3 questions designed to detect temporal leakage.
    
    These questions test if models use future knowledge when answering
    about past versions. The key is to ask about entities/features
    that DON'T exist yet at a given point in time.
    """
    questions = []
    entity_index = _get_valuable_entities(entity_timeline.get("entity_index", {}))
    entity_events = entity_timeline.get("entity_events", [])
    
    if not entity_index or len(commits) < 2:
        return questions
    
    # Sort commits chronologically
    sorted_commits = sorted(commits, key=lambda c: c["timestamp"])
    
    # 1. Assumption check questions - ask about future entities at past versions
    assumption_count = 0
    for i, commit in enumerate(sorted_commits[:-1]):
        if assumption_count >= max_per_category:
            break
        
        commit_hash = commit["hash"]
        tags = commit.get("tags", [])
        version_ctx = _format_version_for_context(commit_hash, tags)
        
        # Find entities introduced AFTER this commit
        future_entities = []
        for entity_key, info in entity_index.items():
            intro_time = info.get("introduced_at", "")
            if intro_time and intro_time > commit["timestamp"]:
                future_entities.append((entity_key, info))
        
        # Ask about future entities at this past version
        for entity_key, info in random.sample(future_entities, min(2, len(future_entities))):
            intro_tags = info.get("introduced_tags", [])
            intro_version = _format_version(info.get("introduced_in", ""), intro_tags)
            
            template = random.choice(RQ3_TEMPLATES["assumption_check"])
            question_text = template["template"].format(
                name=info["name"],
                feature=info["name"],
                capability=_infer_concept_from_name(info["name"]),
                version=version_ctx,
            )
            
            answer_text = (
                f"No, `{info['name']}` does not exist yet at this point. "
                f"It was introduced later in {intro_version}."
            )
            
            # Generate C1, C2, C3 variants for temporal leakage testing
            for context_setting in ["C1", "C2", "C3"]:
                questions.append({
                    "rq": "RQ3",
                    "category": "assumption_check",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "exists": False,
                        "reason": "not_yet_introduced",
                        "introduced_at": info.get("introduced_at"),
                        "introduced_in": info.get("introduced_in"),
                    },
                    "commit": commit_hash,
                    "timestamp": commit.get("timestamp"),
                    "tags": tags,
                    "entity_key": entity_key,
                    "context_setting": context_setting,  # C1: snapshot-only, C2: history-up-to-A, C3: no context
                    "metadata": {
                        "entity_type": info["type"],
                        "entity_name": info["name"],
                        "question_type": "temporal_leakage_test",
                        "temporal_direction": "future_knowledge",
                    },
                })
            assumption_count += 1
    
    # 2. Pre-feature boundary questions
    prefeat_count = 0
    for entity_key, info in entity_index.items():
        if prefeat_count >= max_per_category:
            break
        
        intro_commit = info.get("introduced_in")
        intro_time = info.get("introduced_at")
        
        if not intro_commit:
            continue
        
        # Find a commit BEFORE this entity was introduced
        earlier_commits = [c for c in sorted_commits if c["timestamp"] < intro_time]
        if not earlier_commits:
            continue
        
        earlier = random.choice(earlier_commits)
        earlier_version = _format_version_for_context(earlier["hash"], earlier.get("tags", []))
        
        template = random.choice(RQ3_TEMPLATES["pre_feature_boundary"])
        question_text = template["template"].format(
            feature=info["name"],
            related_task=_infer_concept_from_name(info["name"]),
            version=earlier_version,
        )
        
        answer_text = (
            f"Before `{info['name']}` was introduced, this functionality was not available "
            f"in the library. Users would need to implement it themselves or use external solutions."
        )
        
        # Generate C1, C2, C3 variants
        for context_setting in ["C1", "C2", "C3"]:
            questions.append({
                "rq": "RQ3",
                "category": "pre_feature_boundary",
                "question": question_text,
                "answer": answer_text,
                "ground_truth": {
                    "feature_exists": False,
                    "feature_introduced_at": intro_time,
                    "feature_introduced_in": intro_commit,
                },
                "commit": earlier["hash"],
                "timestamp": earlier["timestamp"],
                "tags": earlier.get("tags", []),
                "entity_key": entity_key,
                "context_setting": context_setting,
                "metadata": {
                    "entity_type": info["type"],
                    "entity_name": info["name"],
                    "question_type": "temporal_leakage_test",
                },
            })
        prefeat_count += 1
    
    # 3. Historical behavior questions - for entities that exist
    hist_count = 0
    for commit in sorted_commits:
        if hist_count >= max_per_category:
            break
        
        commit_hash = commit["hash"]
        tags = commit.get("tags", [])
        version_ctx = _format_version_for_context(commit_hash, tags)
        
        entities_in_version = version_entities.get(commit_hash, [])
        valuable_in_version = [e for e in entities_in_version if e in entity_index]
        
        for entity_key in random.sample(valuable_in_version, min(2, len(valuable_in_version))):
            info = entity_index[entity_key]
            
            template = random.choice(RQ3_TEMPLATES["historical_behavior"])
            question_text = template["template"].format(
                name=info["name"],
                version=version_ctx,
            )
            
            # Get signature if available from events
            signature = None
            for event in entity_events:
                if event.get("entity_key") == entity_key:
                    signature = event.get("signature") or event.get("new_signature")
                    if signature:
                        break
            
            answer_text = f"`{info['name']}` is a {info['type']} that "
            if signature:
                answer_text += f"has the signature `{signature}`. "
            else:
                answer_text += f"is defined in `{info.get('files', ['unknown'])[0]}`. "
            answer_text += f"It was introduced in {_format_version(info.get('introduced_in', ''), info.get('introduced_tags', []))}."
            
            # Generate C1, C2, C3 variants
            for context_setting in ["C1", "C2", "C3"]:
                questions.append({
                    "rq": "RQ3",
                    "category": "historical_behavior",
                    "question": question_text,
                    "answer": answer_text,
                    "ground_truth": {
                        "exists": True,
                        "entity_key": entity_key,
                        "signature": signature,
                    },
                    "commit": commit_hash,
                    "timestamp": commit.get("timestamp"),
                    "tags": tags,
                    "entity_key": entity_key,
                    "context_setting": context_setting,
                    "metadata": {
                        "entity_type": info["type"],
                        "entity_name": info["name"],
                        "question_type": "factual",
                    },
                })
            hist_count += 1
    
    return questions


def _generate_rq4_questions(
    entity_timeline: Dict,
    change_history: Dict,
    commits: List[Dict],
    max_per_category: int = 10,
) -> List[Dict]:
    """Generate RQ4 questions about multi-version reasoning."""
    questions = []
    entity_index = _get_valuable_entities(entity_timeline.get("entity_index", {}))
    
    if not entity_index or len(commits) < 2:
        return questions
    
    sorted_commits = sorted(commits, key=lambda c: c["timestamp"])
    first_version = _format_version(sorted_commits[0]["hash"], sorted_commits[0].get("tags", []))
    last_version = _format_version(sorted_commits[-1]["hash"], sorted_commits[-1].get("tags", []))
    
    # 1. Evolution overview questions for entities with modifications
    evo_count = 0
    for entity_key, info in entity_index.items():
        if evo_count >= max_per_category:
            break
        
        modifications = info.get("modifications", [])
        exists_in = info.get("exists_in_versions", [])
        
        # Only generate for entities that span multiple versions
        if len(exists_in) < 2:
            continue
        
        template = random.choice(RQ4_TEMPLATES["evolution_overview"])
        question_text = template["template"].format(
            name=info["name"],
        )
        
        intro_version = _format_version(
            info.get("introduced_in", ""),
            info.get("introduced_tags", [])
        )
        
        answer_parts = [f"`{info['name']}` was introduced in {intro_version}."]
        
        if modifications:
            answer_parts.append(f"It has been modified {len(modifications)} time(s).")
            for mod in modifications[:3]:  # Limit to first 3 modifications
                mod_version = _format_version(
                    mod.get("commit", ""),
                    []  # We don't have tags for modifications
                )
                changes = mod.get("changes", {})
                if changes.get("signature_changed"):
                    answer_parts.append(f"Signature changed in {mod_version}.")
                elif changes.get("docstring_changed"):
                    answer_parts.append(f"Documentation updated in {mod_version}.")
        else:
            answer_parts.append("It has remained stable without significant modifications.")
        
        if info.get("removed_in"):
            removed_version = _format_version(info["removed_in"], [])
            answer_parts.append(f"It was removed in {removed_version}.")
        
        questions.append({
            "rq": "RQ4",
            "category": "evolution_overview",
            "question": question_text,
            "answer": " ".join(answer_parts),
            "ground_truth": {
                "introduced_in": info.get("introduced_in"),
                "modification_count": len(modifications),
                "modifications": modifications,
                "removed_in": info.get("removed_in"),
                "exists_in_versions": exists_in,
            },
            "version_range": {
                "start": sorted_commits[0]["hash"],
                "end": sorted_commits[-1]["hash"],
            },
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": "summary",
            },
        })
        evo_count += 1
    
    # 2. Compatibility decision questions for signature changes
    compat_count = 0
    for version_change in change_history.get("change_history", []):
        if compat_count >= max_per_category:
            break
        
        old_commit = version_change["old_commit"]
        new_commit = version_change["new_commit"]
        old_version = _format_version(old_commit, [])
        new_version = _format_version(new_commit, [])
        
        for change in version_change.get("changes", []):
            if change.get("change_type") != "signature_changed":
                continue
            
            name = change.get("name")
            if any(re.match(p, name or "") for p in LOW_VALUE_PATTERNS):
                continue
            
            old_args = set(change.get("old_args", []))
            new_args = set(change.get("new_args", []))
            removed_args = old_args - new_args
            added_args = new_args - old_args
            
            is_compatible = len(removed_args) == 0
            
            template = random.choice(RQ4_TEMPLATES["compatibility_decision"])
            question_text = template["template"].format(
                name=name,
                old_version=old_version,
                new_version=new_version,
            )
            
            if is_compatible:
                answer_text = (
                    f"Yes, `{name}` is backward compatible. "
                    f"The new version only added optional parameters: {', '.join(added_args) or 'none'}. "
                    f"Existing code should continue to work."
                )
            else:
                answer_text = (
                    f"No, `{name}` is NOT backward compatible. "
                    f"The following parameters were removed: {', '.join(removed_args)}. "
                    f"Code using these parameters will need to be updated."
                )
            
            questions.append({
                "rq": "RQ4",
                "category": "compatibility_decision",
                "question": question_text,
                "answer": answer_text,
                "ground_truth": {
                    "backward_compatible": is_compatible,
                    "removed_args": list(removed_args),
                    "added_args": list(added_args),
                },
                "before_commit": old_commit,
                "after_commit": new_commit,
                "metadata": {
                    "entity_name": name,
                    "question_type": "analysis",
                },
            })
            compat_count += 1
    
    # 3. Change localization questions
    loc_count = 0
    for entity_key, info in entity_index.items():
        if loc_count >= max_per_category:
            break
        
        modifications = info.get("modifications", [])
        if not modifications:
            continue
        
        # Find period with most changes
        template = random.choice(RQ4_TEMPLATES["change_localization"])
        question_text = template["template"].format(
            name=info["name"],
        )
        
        if len(modifications) == 1:
            mod = modifications[0]
            mod_version = _format_version(mod.get("commit", ""), [])
            answer_text = f"The only modification to `{info['name']}` occurred in {mod_version}."
        else:
            first_mod = modifications[0]
            last_mod = modifications[-1]
            first_mod_version = _format_version(first_mod.get("commit", ""), [])
            last_mod_version = _format_version(last_mod.get("commit", ""), [])
            answer_text = (
                f"`{info['name']}` was modified {len(modifications)} times, "
                f"spanning from {first_mod_version} to {last_mod_version}."
            )
        
        questions.append({
            "rq": "RQ4",
            "category": "change_localization",
            "question": question_text,
            "answer": answer_text,
            "ground_truth": {
                "modification_count": len(modifications),
                "modifications": modifications,
            },
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": "factual",
            },
        })
        loc_count += 1
    
    # 4. Regression analysis questions (for removed entities)
    reg_count = 0
    for entity_key, info in entity_index.items():
        if reg_count >= max_per_category:
            break
        
        if not info.get("removed_in"):
            continue
        
        template = random.choice(RQ4_TEMPLATES["regression_analysis"])
        question_text = template["template"].format(
            name=info["name"],
        )
        
        removed_version = _format_version(info["removed_in"], [])
        answer_text = (
            f"`{info['name']}` was removed in {removed_version} "
            f"(at {info.get('removed_at', 'unknown time')[:10]}). "
            f"If your code depends on it, you may need to find an alternative or pin to an earlier version."
        )
        
        questions.append({
            "rq": "RQ4",
            "category": "regression_analysis",
            "question": question_text,
            "answer": answer_text,
            "ground_truth": {
                "removed_in": info["removed_in"],
                "removed_at": info.get("removed_at"),
                "introduced_in": info.get("introduced_in"),
            },
            "metadata": {
                "entity_type": info["type"],
                "entity_name": info["name"],
                "question_type": "analysis",
            },
        })
        reg_count += 1
    
    return questions


# =============================================================================
# Main Generation Function
# =============================================================================

def generate_qa_dataset(
    entity_timeline_path: str,
    change_history_path: str,
    index_path: str,
    snapshots_dir: str,
    output_path: str,
    max_questions_per_rq: int = 100,
    seed: int = 42,
) -> Dict:
    """Generate the complete Q&A dataset from pipeline outputs.
    
    Args:
        entity_timeline_path: Path to entity_timeline.json
        change_history_path: Path to change_history.json
        index_path: Path to index.json
        snapshots_dir: Path to snapshots directory
        output_path: Output path for generated Q&A dataset
        max_questions_per_rq: Maximum questions per research question
        seed: Random seed for reproducibility
    """
    random.seed(seed)
    
    # Load required data
    with open(entity_timeline_path, "r", encoding="utf-8") as f:
        entity_timeline = json.load(f)
    
    with open(change_history_path, "r", encoding="utf-8") as f:
        change_history = json.load(f)
    
    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)
    
    commits = index_data.get("commits", [])
    version_entities = entity_timeline.get("version_entities", {})
    
    # Load parsed data for each snapshot
    snapshots_path = Path(snapshots_dir)
    parsed_data: Dict[str, Dict] = {}
    for commit in commits:
        parsed_path = snapshots_path / commit["hash"] / "parsed.json"
        if parsed_path.exists():
            with open(parsed_path, "r", encoding="utf-8") as f:
                parsed_data[commit["hash"]] = json.load(f)
    
    # Calculate max per category based on max per RQ
    max_per_category = max(5, max_questions_per_rq // 5)
    
    # Generate questions for each RQ
    all_questions = []
    
    print("Generating RQ1 questions...")
    rq1_questions = _generate_rq1_questions(
        entity_timeline, version_entities, parsed_data, commits,
        max_per_category=max_per_category
    )
    all_questions.extend(rq1_questions)
    print(f"  Generated {len(rq1_questions)} RQ1 questions")
    
    print("Generating RQ2 questions...")
    rq2_questions = _generate_rq2_questions(
        change_history, entity_timeline,
        max_per_category=max_per_category
    )
    all_questions.extend(rq2_questions)
    print(f"  Generated {len(rq2_questions)} RQ2 questions")
    
    print("Generating RQ3 questions...")
    rq3_questions = _generate_rq3_questions(
        entity_timeline, version_entities, commits,
        max_per_category=max_per_category
    )
    all_questions.extend(rq3_questions)
    print(f"  Generated {len(rq3_questions)} RQ3 questions")
    
    print("Generating RQ4 questions...")
    rq4_questions = _generate_rq4_questions(
        entity_timeline, change_history, commits,
        max_per_category=max_per_category
    )
    all_questions.extend(rq4_questions)
    print(f"  Generated {len(rq4_questions)} RQ4 questions")
    
    # Add unique IDs
    for i, q in enumerate(all_questions):
        q["id"] = f"q_{i:05d}"
    
    # Build summary
    rq_counts = {}
    category_counts = {}
    for q in all_questions:
        rq = q.get("rq", "unknown")
        cat = q.get("category", "unknown")
        rq_counts[rq] = rq_counts.get(rq, 0) + 1
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    result = {
        "summary": {
            "total_questions": len(all_questions),
            "questions_by_rq": rq_counts,
            "questions_by_category": category_counts,
            "seed": seed,
            "entity_filtering": {
                "excluded_patterns": LOW_VALUE_PATTERNS,
                "excluded_files": LOW_VALUE_FILES,
            },
        },
        "questions": all_questions,
    }
    
    # Write output
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Q&A dataset from pipeline outputs.")
    parser.add_argument("--entity_timeline", required=True, help="Path to entity_timeline.json")
    parser.add_argument("--change_history", required=True, help="Path to change_history.json")
    parser.add_argument("--index_path", required=True, help="Path to index.json")
    parser.add_argument("--snapshots_dir", required=True, help="Path to snapshots directory")
    parser.add_argument("--out", required=True, help="Output path for Q&A dataset")
    parser.add_argument("--max_questions", type=int, default=100, help="Max questions per RQ")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    
    result = generate_qa_dataset(
        args.entity_timeline,
        args.change_history,
        args.index_path,
        args.snapshots_dir,
        args.out,
        max_questions_per_rq=args.max_questions,
        seed=args.seed,
    )
    
    print(f"\nGenerated {result['summary']['total_questions']} questions")
    print(f"Questions by RQ: {result['summary']['questions_by_rq']}")
    print(f"Questions by category: {result['summary']['questions_by_category']}")
    print(f"Wrote Q&A dataset to {args.out}")

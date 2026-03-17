"""
Module: ast_parser.py

Purpose: Parse source code to extract QA-enriched symbol-level information
(functions, classes, imports, module docstrings). Does NOT store full AST
trees - only extracts symbols and discards the rest immediately.

All parsing flows through the unified `parse_file_for_qa()` entrypoint.
The legacy `parse_file()` and `parse_slice_files()` are kept as thin wrappers
for backwards-compatibility but delegate to their QA equivalents.

Key Functions:
- parse_file(file_path, language) -> Optional[Dict]
  Wrapper around parse_file_for_qa(); returns QA-enriched symbol data.
- parse_file_for_qa(file_path, language) -> Optional[Dict]
  Extracts: functions (typed params, return type, decorators, doc),
            classes (fields, method list, decorators, doc),
            imports, module_doc.
- parse_slice_files(repo_path, commit, extensions, timeout) -> List[Dict]
  Wrapper around parse_slice_files_for_qa().
- parse_slice_files_for_qa(repo_path, commit, extensions, timeout) -> List[Dict]
  Parses all source files at a commit using QA-enriched extraction.

Example:
    >>> result = parse_file("src/main.py", "python")
    >>> if result:
    ...     print(f"Functions: {len(result['functions'])}")
    ...     print(f"Classes: {len(result['classes'])}")
    ...     print(f"Imports: {len(result['imports'])}")
"""

import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
import tree_sitter
from tree_sitter import Language, Parser

logger = logging.getLogger(__name__)

# Tree-sitter language modules (will be loaded dynamically)
_languages = {}


# ===========================================================================
# REMOVED: Legacy API-break-detection helpers
# The following functions were removed as part of the metadata consolidation:
#   _should_traverse_node()           - replaced by _should_traverse_node_qa()
#   _extract_functions_internal()     - replaced by _extract_functions_for_qa()
#   _extract_function_symbol()        - replaced by _extract_function_symbol_qa()
#   _extract_classes_internal()       - replaced by _extract_classes_for_qa()
#   _extract_class_symbol()           - replaced by _extract_class_symbol_qa()
#   _extract_comments_internal()      - docstrings are now inlined in function/class doc field
#   _extract_python_docstrings()      - see above
#   _extract_java_javadoc()           - see above
# Shared helpers still used by the QA path are kept below:
#   _find_docstring_in_node()         - used by _extract_function_symbol_qa
#   _find_string_literal()            - used by _find_docstring_in_node
#   _clean_docstring_text()           - used by _find_docstring_in_node
#   _find_javadoc_before_node()       - used by _extract_function_symbol_qa
#   _clean_javadoc_text()             - used by _find_javadoc_before_node
# ===========================================================================



def _load_language(lang_name: str) -> Optional[Language]:
    """
    Load tree-sitter language module.
    
    Args:
        lang_name: Language name (python, java)
        
    Returns:
        Language object, or None if error
    """
    if lang_name in _languages:
        return _languages[lang_name]
    
    try:
        # Try to load from installed packages
        # tree-sitter language bindings return a PyCapsule that needs to be wrapped in Language()
        if lang_name == "python":
            import tree_sitter_python
            lang = Language(tree_sitter_python.language())
        elif lang_name == "java":
            import tree_sitter_java
            lang = Language(tree_sitter_java.language())
        else:
            logger.error(f"Unsupported language: {lang_name}")
            return None
        
        _languages[lang_name] = lang
        return lang
        
    except ImportError as e:
        logger.error(f"Failed to import tree-sitter language for {lang_name}: {e}")
        logger.error(f"Please install tree-sitter language bindings: pip install tree-sitter-{lang_name}")
        return None
    except Exception as e:
        logger.error(f"Error loading language {lang_name}: {e}")
        return None


def detect_language(file_path: str, config_extensions: Dict[str, List[str]]) -> Optional[str]:
    """
    Detect programming language from file extension.
    
    Args:
        file_path: Path to file
        config_extensions: Mapping of language to extensions
        
    Returns:
        Language name, or None if not detected
    """
    ext = Path(file_path).suffix.lower()
    
    for lang, extensions in config_extensions.items():
        if ext in extensions:
            return lang
    
    return None


def parse_file(
    file_path: str,
    language: str,
    timeout_seconds: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Parse a source code file and extract QA-enriched symbol-level metadata.

    This is a thin wrapper around ``parse_file_for_qa()`` kept for
    backwards-compatibility.  All extraction now flows through the unified
    QA parser, so callers receive richer data than the old API-break path:
    typed parameters, return types, decorators, class fields, imports, and
    an inline docstring on each function/class symbol.

    Args:
        file_path: Path to source file
        language: Programming language ("python" or "java")
        timeout_seconds: Timeout for parsing

    Returns:
        Dictionary containing QA-enriched symbol data:
        {
            "file_path": str,
            "content_hash": str,
            "language": str,
            "functions": List[Dict],   # typed params, decorators, doc, ...
            "classes": List[Dict],     # fields, methods list, doc, ...
            "imports": List[Dict],
            "module_doc": Optional[str]
        }
        Returns None on error or if file does not exist.
    """
    return parse_file_for_qa(file_path, language, timeout_seconds)


def _find_docstring_in_node(node: tree_sitter.Node, content: bytes) -> Optional[str]:
    """
    Find docstring (first string literal) in a function or class body.
    
    Args:
        node: Function or class definition node
        content: Source code bytes
        
    Returns:
        Docstring text (cleaned) or None if not found
    """
    # Find the body of the function/class
    body = None
    for child in node.children:
        if child.type in ["block", "suite"]:  # Python function/class body
            body = child
            break
    
    if not body:
        return None
    
    # Look for the first expression_statement containing a string literal
    for child in body.children:
        if child.type == "expression_statement":
            # Check if it contains a string literal
            string_node = _find_string_literal(child)
            if string_node:
                # Extract and clean the docstring
                docstring_text = content[string_node.start_byte:string_node.end_byte].decode('utf-8', errors='ignore')
                return _clean_docstring_text(docstring_text)
    
    return None


def _find_string_literal(node: tree_sitter.Node) -> Optional[tree_sitter.Node]:
    """
    Recursively find the first string literal node.
    """
    if node.type in ["string", "concatenated_string"]:
        return node
    
    for child in node.children:
        result = _find_string_literal(child)
        if result:
            return result
    
    return None


def _clean_docstring_text(text: str) -> str:
    """
    Clean Python docstring text (remove triple quotes).
    """
    text = text.strip()
    # Remove opening triple quotes
    if text.startswith('"""'):
        text = text[3:]
    elif text.startswith("'''"):
        text = text[3:]
    # Remove closing triple quotes
    if text.endswith('"""'):
        text = text[:-3]
    elif text.endswith("'''"):
        text = text[:-3]
    return text.strip()



def _find_javadoc_before_node(node: tree_sitter.Node, content: bytes) -> Optional[str]:
    """
    Find Javadoc comment (/** ... */) immediately before a node.
    Checks siblings before the node in the parent's children list.
    
    Args:
        node: Node to find Javadoc for
        content: Source code bytes
        
    Returns:
        Javadoc text (cleaned) or None if not found
    """
    if not node.parent:
        return None
    
    # Find this node's index in parent's children
    parent = node.parent
    node_index = -1
    for i, child in enumerate(parent.children):
        if child == node:
            node_index = i
            break
    
    if node_index <= 0:
        return None
    
    # Check previous siblings for Javadoc comment
    for i in range(node_index - 1, -1, -1):
        sibling = parent.children[i]
        if sibling.type in ["block_comment", "comment"]:
            comment_text = content[sibling.start_byte:sibling.end_byte].decode('utf-8', errors='ignore')
            # Check if it's Javadoc (starts with /**)
            if comment_text.strip().startswith('/**'):
                return _clean_javadoc_text(comment_text)
        # Skip whitespace/newlines, but stop at other significant nodes
        elif sibling.type not in ["\n", "line_break"]:
            break
    
    return None


def _clean_javadoc_text(text: str) -> str:
    """
    Clean Java Javadoc text (remove /** and */ markers).
    """
    text = text.strip()
    # Remove opening /**
    if text.startswith('/**'):
        text = text[3:]
    # Remove closing */
    if text.endswith('*/'):
        text = text[:-2]
    # Remove leading * from each line (common Javadoc style)
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('*'):
            cleaned_lines.append(stripped[1:].lstrip())
        else:
            cleaned_lines.append(line)
    return '\n'.join(cleaned_lines).strip()


# Removed unused functions: _clean_comment_text, _determine_comment_kind, _find_comment_owner
# These were replaced by language-specific extraction functions for docstrings and Javadoc only


def calculate_content_hash(content: bytes) -> str:
    """
    Calculate SHA256 hash of file content.
    
    Args:
        content: File content bytes
        
    Returns:
        Hexadecimal hash string
    """
    return hashlib.sha256(content).hexdigest()


def parse_slice_files(
    repo_path: str,
    slice_commit_hash: str,
    config_extensions: Dict[str, List[str]],
    timeout_seconds: int = 30
) -> List[Dict[str, Any]]:
    """
    Parse all source files in a repository at a specific commit.

    Thin wrapper around ``parse_slice_files_for_qa()`` kept for
    backwards-compatibility.  Returns QA-enriched data.

    Args:
        repo_path: Path to repository
        slice_commit_hash: Commit hash to checkout
        config_extensions: Language to extension mapping
        timeout_seconds: Parsing timeout

    Returns:
        List of QA-enriched parsed file information
    """
    return parse_slice_files_for_qa(
        repo_path, slice_commit_hash, config_extensions, timeout_seconds
    )


# ============================================================
# Unified QA-enriched parsing
# These are the authoritative parsing implementations.
# parse_file() and parse_slice_files() above are thin wrappers.
# ============================================================


def _should_traverse_node_qa(node_type: str, language: str) -> bool:
    """
    QA-specific traversal predicate. Unlike _should_traverse_node(),
    this also traverses block/suite/decorated_definition nodes so that
    methods inside class bodies are discovered.
    """
    if language == "python":
        return node_type in {
            "module",
            "class_definition",
            "function_definition",
            "block",
            "suite",
            "decorated_definition",
        }
    elif language == "java":
        return node_type in {
            "program",
            "compilation_unit",
            "class_declaration",
            "interface_declaration",
            "class_body",
            "interface_body",
            "method_declaration",
            "constructor_declaration",
        }
    return False


def parse_file_for_qa(
    file_path: str,
    language: str,
    timeout_seconds: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Parse a source code file and extract QA-enriched metadata.
    Returns richer data than parse_file(): parameter types, return types,
    decorators, class fields, imports, and module-level docstrings.

    Does NOT modify or replace the existing parse_file() function.

    Args:
        file_path: Path to source file
        language: Programming language
        timeout_seconds: Timeout for parsing

    Returns:
        Dictionary containing QA-enriched data:
        {
            "file_path": str,
            "content_hash": str,
            "language": str,
            "functions": List[Dict],   # QA-enriched function symbols
            "classes": List[Dict],     # QA-enriched class symbols
            "imports": List[Dict],     # Import statements
            "module_doc": Optional[str] # Module-level docstring
        }
        Returns None if error
    """
    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.warning(f"File not found: {file_path}")
            return None

        with open(file_path, 'rb') as f:
            content = f.read()

        if not content:
            return None

        lang = _load_language(language)
        if not lang:
            return None

        parser = Parser(lang)
        tree = parser.parse(content)

        functions = _extract_functions_for_qa(tree, content, language, file_path)
        classes = _extract_classes_for_qa(tree, content, language, file_path)
        imports = _extract_imports(tree, content, language)
        module_doc = _extract_module_doc(tree, content, language)
        content_hash = calculate_content_hash(content)

        return {
            "file_path": file_path,
            "content_hash": content_hash,
            "language": language,
            "functions": functions,
            "classes": classes,
            "imports": imports,
            "module_doc": module_doc
        }

    except Exception as e:
        logger.warning(f"Error parsing file for QA {file_path}: {e}")
        return None


# --- QA Function Extraction ---


def _extract_functions_for_qa(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str,
    file_path: str
) -> List[Dict[str, Any]]:
    """
    Extract QA-enriched function symbols from AST.
    Includes parameter types, return types, decorators, and inline docstrings.
    """
    functions = []

    function_types = {
        "python": ["function_definition"],
        "java": ["method_declaration", "constructor_declaration"]
    }
    target_types = function_types.get(language, [])

    def traverse(node: tree_sitter.Node, container: Optional[str] = None):
        if node.type in target_types:
            func_info = _extract_function_symbol_qa(node, language, content, container, file_path)
            if func_info:
                functions.append(func_info)

        current_container = container
        if language == "python" and node.type == "class_definition":
            for child in node.children:
                if child.type == "identifier":
                    current_container = child.text.decode('utf-8', errors='ignore')
                    break
        elif language == "java" and node.type in ["class_declaration", "interface_declaration"]:
            for child in node.children:
                if child.type in {"type_identifier", "identifier"}:
                    current_container = child.text.decode('utf-8', errors='ignore')
                    break

        for child in node.children:
            if _should_traverse_node_qa(child.type, language):
                traverse(child, current_container)

    traverse(tree.root_node)
    return functions


def _extract_function_symbol_qa(
    node: tree_sitter.Node,
    language: str,
    content: bytes,
    container: Optional[str],
    file_path: str
) -> Optional[Dict[str, Any]]:
    """
    Extract QA-enriched function/method symbol.
    Adds: typed parameters, Python return type, decorators, docstring association.
    """
    # --- Name ---
    name = None
    for child in node.children:
        if child.type == "identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break

    if not name:
        return None

    # --- Kind ---
    if container:
        kind = "method"
        if language == "java" and name == container:
            kind = "constructor"
    else:
        kind = "function"

    # --- Signature ---
    start_line = node.start_point[0] + 1
    end_line = node.end_point[0] + 1
    if language == "java":
        signature = _extract_java_method_signature(node, content)
    else:
        start_byte = node.start_byte
        first_line_end = content.find(b'\n', start_byte)
        if first_line_end == -1 or first_line_end > node.end_byte:
            first_line_end = node.end_byte
        signature = content[start_byte:first_line_end].decode('utf-8', errors='ignore').strip()

    # --- Parameters (with types and defaults) ---
    if language == "python":
        parameters = _extract_python_params_qa(node, content)
    elif language == "java":
        parameters = _extract_java_params_qa(node, content)
    else:
        parameters = []

    # --- Return type ---
    return_type = None
    if language == "python":
        return_type = _extract_python_return_type(node, content)
    elif language == "java":
        java_return_type_nodes = {
            "type_identifier", "void", "void_type", "integral_type",
            "floating_point_type", "boolean_type", "generic_type",
            "scoped_type", "array_type",
        }
        for child in node.children:
            if child.type in java_return_type_nodes:
                return_type = child.text.decode('utf-8', errors='ignore')
                break

    # --- Visibility and static (Java) ---
    visibility = None
    is_static = False
    is_abstract = False
    if language == "java":
        for child in node.children:
            if child.type == "modifiers":
                mod_text = child.text.decode('utf-8', errors='ignore').split()
                if "public" in mod_text:
                    visibility = "public"
                elif "protected" in mod_text:
                    visibility = "protected"
                elif "private" in mod_text:
                    visibility = "private"
                if "static" in mod_text:
                    is_static = True
                if "abstract" in mod_text:
                    is_abstract = True
        if visibility is None:
            visibility = "package"
    elif language == "python":
        # Convention-based visibility
        if name.startswith('__') and not name.endswith('__'):
            visibility = "private"
        elif name.startswith('_'):
            visibility = "protected"
        else:
            visibility = "public"

    # --- Decorators ---
    decorators = _extract_decorators(node, language, content)
    if "staticmethod" in decorators:
        is_static = True
    if "abstractmethod" in decorators:
        is_abstract = True

    # --- Docstring ---
    doc = None
    if language == "python":
        doc = _find_docstring_in_node(node, content)
    elif language == "java":
        doc = _find_javadoc_before_node(node, content)

    return {
        "name": name,
        "kind": kind,
        "container": container,
        "signature": signature,
        "parameters": parameters,
        "return_type": return_type,
        "decorators": decorators,
        "visibility": visibility,
        "is_static": is_static,
        "is_abstract": is_abstract,
        "start_line": start_line,
        "end_line": end_line,
        "doc": doc,
        "file": file_path
    }


def _extract_java_method_signature(
    node: tree_sitter.Node,
    content: bytes
) -> str:
    """
    Extract Java method/constructor declaration signature without body.
    Removes leading annotations and normalizes whitespace.
    """
    raw = content[node.start_byte:node.end_byte].decode('utf-8', errors='ignore')

    # Keep declaration only (before method body or abstract ';').
    cut_positions = [pos for pos in (raw.find('{'), raw.find(';')) if pos != -1]
    if cut_positions:
        declaration = raw[:min(cut_positions)].strip()
    else:
        declaration = raw.strip()

    # Drop leading annotations like @Test, @SuppressWarnings("x").
    trimmed = declaration.lstrip()
    while trimmed.startswith('@'):
        idx = 1
        paren_depth = 0
        while idx < len(trimmed):
            ch = trimmed[idx]
            if ch == '(':
                paren_depth += 1
            elif ch == ')' and paren_depth > 0:
                paren_depth -= 1
            elif paren_depth == 0 and ch.isspace():
                break
            idx += 1
        trimmed = trimmed[idx:].lstrip()

    # Normalize whitespace to make signature compact and comparable.
    candidate = trimmed if trimmed else declaration
    return " ".join(candidate.split())


def _extract_python_params_qa(
    node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """
    Extract Python function parameters with type annotations and default values.
    Handles: identifier, typed_parameter, default_parameter, typed_default_parameter,
    list_splat_pattern (*args), dictionary_splat_pattern (**kwargs).
    """
    params = []

    for child in node.children:
        if child.type != "parameters":
            continue

        for param in child.children:
            # Skip parentheses and commas
            if param.type in {"(", ")", ","}:
                continue

            p = _parse_single_python_param(param, content)
            if p:
                params.append(p)

    return params


def _parse_single_python_param(
    param: tree_sitter.Node,
    content: bytes
) -> Optional[Dict[str, Any]]:
    """Parse a single Python parameter node into a QA param dict."""
    name = None
    type_annotation = None
    default_value = None

    if param.type == "identifier":
        name = param.text.decode('utf-8', errors='ignore')

    elif param.type == "typed_parameter":
        for pchild in param.children:
            if pchild.type == "identifier":
                name = pchild.text.decode('utf-8', errors='ignore')
            elif pchild.type == "type":
                type_annotation = pchild.text.decode('utf-8', errors='ignore')

    elif param.type == "default_parameter":
        for pchild in param.children:
            if pchild.type == "identifier":
                name = pchild.text.decode('utf-8', errors='ignore')
            elif pchild.type not in {"=", ":"}:
                if name and not default_value:
                    default_value = pchild.text.decode('utf-8', errors='ignore')

    elif param.type == "typed_default_parameter":
        for pchild in param.children:
            if pchild.type == "identifier" and name is None:
                name = pchild.text.decode('utf-8', errors='ignore')
            elif pchild.type == "type":
                type_annotation = pchild.text.decode('utf-8', errors='ignore')
            elif pchild.type not in {"=", ":", "identifier", "type"} and default_value is None:
                default_value = pchild.text.decode('utf-8', errors='ignore')

    elif param.type == "list_splat_pattern":
        for pchild in param.children:
            if pchild.type == "identifier":
                name = "*" + pchild.text.decode('utf-8', errors='ignore')

    elif param.type == "dictionary_splat_pattern":
        for pchild in param.children:
            if pchild.type == "identifier":
                name = "**" + pchild.text.decode('utf-8', errors='ignore')

    if not name:
        return None

    return {
        "name": name,
        "type_annotation": type_annotation,
        "default_value": default_value
    }


def _extract_java_params_qa(
    node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """
    Extract Java method parameters with types.
    """
    params = []

    for child in node.children:
        if child.type != "formal_parameters":
            continue

        for param in child.children:
            if param.type == "formal_parameter":
                p_name = None
                p_type = None
                for pchild in param.children:
                    if pchild.type == "identifier":
                        p_name = pchild.text.decode('utf-8', errors='ignore')
                    elif pchild.type in {
                        "type_identifier", "integral_type", "floating_point_type",
                        "boolean_type", "void_type", "generic_type",
                        "scoped_type", "array_type"
                    }:
                        p_type = pchild.text.decode('utf-8', errors='ignore')

                if p_name:
                    params.append({
                        "name": p_name,
                        "type_annotation": p_type,
                        "default_value": None
                    })

            elif param.type == "spread_parameter":
                # varargs: Type... name
                p_name = None
                p_type = None
                for pchild in param.children:
                    if pchild.type == "identifier":
                        p_name = pchild.text.decode('utf-8', errors='ignore')
                    elif pchild.type in {
                        "type_identifier", "integral_type", "generic_type",
                        "scoped_type", "array_type"
                    }:
                        p_type = pchild.text.decode('utf-8', errors='ignore') + "..."

                if p_name:
                    params.append({
                        "name": p_name,
                        "type_annotation": p_type,
                        "default_value": None
                    })

    return params


def _extract_python_return_type(
    node: tree_sitter.Node,
    content: bytes
) -> Optional[str]:
    """
    Extract Python return-type annotation (the `-> X` part).
    In tree-sitter-python the return type appears as a `type` child
    directly under `function_definition`.
    """
    for child in node.children:
        if child.type == "type":
            return child.text.decode('utf-8', errors='ignore')
    return None


def _extract_decorators(
    node: tree_sitter.Node,
    language: str,
    content: bytes
) -> List[str]:
    """
    Extract decorator names from a function or class node.
    Python: `decorated_definition` wraps the node with `decorator` children.
    Java:  `modifiers` may contain `annotation` children (e.g. @Override).
    """
    decorators = []

    if language == "python":
        # In tree-sitter-python, decorators are children of `decorated_definition`
        # which is the parent of the actual function/class node.
        parent = node.parent
        if parent and parent.type == "decorated_definition":
            for child in parent.children:
                if child.type == "decorator":
                    # Extract decorator text after '@'
                    dec_text = child.text.decode('utf-8', errors='ignore').strip()
                    if dec_text.startswith('@'):
                        dec_text = dec_text[1:]
                    # Strip arguments: @decorator(args) → decorator
                    paren_idx = dec_text.find('(')
                    if paren_idx != -1:
                        dec_text = dec_text[:paren_idx]
                    decorators.append(dec_text.strip())

    elif language == "java":
        for child in node.children:
            if child.type == "modifiers":
                for mod_child in child.children:
                    if mod_child.type in {"annotation", "marker_annotation"}:
                        ann_text = mod_child.text.decode('utf-8', errors='ignore').strip()
                        if ann_text.startswith('@'):
                            ann_text = ann_text[1:]
                        paren_idx = ann_text.find('(')
                        if paren_idx != -1:
                            ann_text = ann_text[:paren_idx]
                        decorators.append(ann_text.strip())

    return decorators


# --- QA Class Extraction ---


def _extract_classes_for_qa(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str,
    file_path: str
) -> List[Dict[str, Any]]:
    """
    Extract QA-enriched class/interface symbols from AST.
    Includes fields, method list, decorators, and docstrings.
    """
    classes = []

    class_types = {
        "python": ["class_definition"],
        "java": ["class_declaration", "interface_declaration"]
    }
    target_types = class_types.get(language, [])

    def traverse(node: tree_sitter.Node):
        if node.type in target_types:
            class_info = _extract_class_symbol_qa(node, language, content, file_path)
            if class_info:
                classes.append(class_info)

        for child in node.children:
            if _should_traverse_node_qa(child.type, language):
                traverse(child)

    traverse(tree.root_node)
    return classes


def _extract_class_symbol_qa(
    node: tree_sitter.Node,
    language: str,
    content: bytes,
    file_path: str
) -> Optional[Dict[str, Any]]:
    """
    Extract QA-enriched class/interface symbol.
    Adds: fields, method list, decorators, docstring, abstract/visibility.
    """
    # --- Name ---
    name = None
    for child in node.children:
        if language == "python" and child.type == "identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break
        elif language == "java" and child.type in {"type_identifier", "identifier"}:
            name = child.text.decode('utf-8', errors='ignore')
            break

    if not name:
        return None

    # --- Kind ---
    kind = "class"
    is_abstract = False
    visibility = None

    if language == "java":
        if node.type == "interface_declaration":
            kind = "interface"
        for child in node.children:
            if child.type == "modifiers":
                mod_text = child.text.decode('utf-8', errors='ignore').split()
                if "abstract" in mod_text:
                    kind = "abstract_class"
                    is_abstract = True
                if "public" in mod_text:
                    visibility = "public"
                elif "protected" in mod_text:
                    visibility = "protected"
                elif "private" in mod_text:
                    visibility = "private"

    # --- Base classes / interfaces ---
    base_classes = []
    implemented_interfaces = []

    if language == "python":
        for child in node.children:
            if child.type == "argument_list":
                for arg in child.children:
                    if arg.type == "identifier":
                        base_classes.append(arg.text.decode('utf-8', errors='ignore'))
                    elif arg.type == "attribute":
                        base_classes.append(arg.text.decode('utf-8', errors='ignore'))
    elif language == "java":
        for child in node.children:
            if child.type == "superclass":
                for schild in child.children:
                    if schild.type in {"type_identifier", "identifier", "generic_type", "scoped_type"}:
                        base_classes.append(schild.text.decode('utf-8', errors='ignore'))
                        break
            elif child.type == "super_interfaces":
                for iface in child.children:
                    if iface.type == "type_list":
                        for iface_child in iface.children:
                            if iface_child.type in {"type_identifier", "identifier", "generic_type", "scoped_type"}:
                                implemented_interfaces.append(iface_child.text.decode('utf-8', errors='ignore'))
                    elif iface.type in {"type_identifier", "identifier", "generic_type", "scoped_type"}:
                        implemented_interfaces.append(iface.text.decode('utf-8', errors='ignore'))

    # --- Decorators ---
    decorators = _extract_decorators(node, language, content)

    # Check Python ABC
    if language == "python":
        if "ABCMeta" in base_classes or "ABC" in base_classes or "abstractmethod" in decorators:
            is_abstract = True

    # --- Fields ---
    if language == "python":
        fields = _extract_python_class_fields(node, content)
    elif language == "java":
        fields = _extract_java_class_fields(node, content)
    else:
        fields = []

    # --- Method list ---
    methods = _extract_class_methods(node, language, content)

    # --- Docstring ---
    doc = None
    if language == "python":
        doc = _find_docstring_in_node(node, content)
    elif language == "java":
        doc = _find_javadoc_before_node(node, content)

    start_line = node.start_point[0] + 1
    end_line = node.end_point[0] + 1

    return {
        "name": name,
        "kind": kind,
        "base_classes": base_classes,
        "implemented_interfaces": implemented_interfaces,
        "decorators": decorators,
        "fields": fields,
        "methods": methods,
        "visibility": visibility,
        "is_abstract": is_abstract,
        "start_line": start_line,
        "end_line": end_line,
        "doc": doc,
        "file": file_path
    }


def _extract_python_class_fields(
    node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """
    Extract Python class-level attributes.
    Looks for assignments in the class body (not inside methods)
    and __init__ self.X assignments.
    """
    fields = []
    seen_names = set()

    # Find class body
    body = None
    for child in node.children:
        if child.type in {"block", "suite"}:
            body = child
            break

    if not body:
        return fields

    for stmt in body.children:
        # Class-level assignments: x = value or x: type = value
        if stmt.type == "expression_statement":
            expr = None
            for ch in stmt.children:
                if ch.type == "assignment":
                    expr = ch
                    break
            if expr:
                field = _parse_python_assignment_as_field(expr, content)
                if field and field["name"] not in seen_names:
                    fields.append(field)
                    seen_names.add(field["name"])

        # Annotated assignment: x: int = 5
        elif stmt.type == "type_alias_statement":
            pass  # skip type aliases

        # Look into __init__ for self.x assignments
        elif stmt.type == "function_definition":
            func_name = None
            for ch in stmt.children:
                if ch.type == "identifier":
                    func_name = ch.text.decode('utf-8', errors='ignore')
                    break
            if func_name == "__init__":
                init_fields = _extract_init_self_fields(stmt, content)
                for f in init_fields:
                    if f["name"] not in seen_names:
                        fields.append(f)
                        seen_names.add(f["name"])

    return fields


def _parse_python_assignment_as_field(
    node: tree_sitter.Node,
    content: bytes
) -> Optional[Dict[str, Any]]:
    """Parse a Python assignment node as a class field."""
    name = None
    type_annotation = None
    default_value = None

    children = list(node.children)
    # Simple assignment: x = value
    if len(children) >= 3 and children[0].type == "identifier":
        name = children[0].text.decode('utf-8', errors='ignore')
        # Last child after '=' is the value
        default_value = children[-1].text.decode('utf-8', errors='ignore')
    # Pattern variables: ignore complex patterns
    elif len(children) >= 1 and children[0].type == "pattern_list":
        return None

    if not name:
        return None

    # Determine visibility by convention
    visibility = "public"
    if name.startswith('__') and not name.endswith('__'):
        visibility = "private"
    elif name.startswith('_'):
        visibility = "protected"

    return {
        "name": name,
        "type_annotation": type_annotation,
        "default_value": default_value,
        "visibility": visibility,
        "is_static": True  # class-level variables are class/static attributes
    }


def _extract_init_self_fields(
    init_node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """
    Extract self.X = ... assignments from __init__ method body.
    """
    fields = []

    # Find body
    body = None
    for child in init_node.children:
        if child.type in {"block", "suite"}:
            body = child
            break

    if not body:
        return fields

    def scan_body(body_node: tree_sitter.Node):
        for stmt in body_node.children:
            if stmt.type == "expression_statement":
                for ch in stmt.children:
                    if ch.type == "assignment":
                        field = _parse_self_assignment(ch, content)
                        if field:
                            fields.append(field)

    scan_body(body)
    return fields


def _parse_self_assignment(
    node: tree_sitter.Node,
    content: bytes
) -> Optional[Dict[str, Any]]:
    """Parse self.x = value as a field."""
    children = list(node.children)
    if len(children) < 3:
        return None

    lhs = children[0]
    if lhs.type != "attribute":
        return None

    lhs_text = lhs.text.decode('utf-8', errors='ignore')
    if not lhs_text.startswith("self."):
        return None

    attr_name = lhs_text[5:]  # strip "self."
    if not attr_name:
        return None

    default_value = children[-1].text.decode('utf-8', errors='ignore')

    visibility = "public"
    if attr_name.startswith('__') and not attr_name.endswith('__'):
        visibility = "private"
    elif attr_name.startswith('_'):
        visibility = "protected"

    return {
        "name": attr_name,
        "type_annotation": None,
        "default_value": default_value,
        "visibility": visibility,
        "is_static": False
    }


def _extract_java_class_fields(
    node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """
    Extract Java class-level field declarations.
    """
    fields = []

    # Find class_body or interface_body
    body = None
    for child in node.children:
        if child.type in {"class_body", "interface_body"}:
            body = child
            break

    if not body:
        return fields

    for stmt in body.children:
        if stmt.type == "field_declaration":
            f_name = None
            f_type = None
            f_default = None
            f_visibility = None
            f_is_static = False

            for fchild in stmt.children:
                if fchild.type == "modifiers":
                    mod_text = fchild.text.decode('utf-8', errors='ignore').split()
                    if "public" in mod_text:
                        f_visibility = "public"
                    elif "protected" in mod_text:
                        f_visibility = "protected"
                    elif "private" in mod_text:
                        f_visibility = "private"
                    if "static" in mod_text:
                        f_is_static = True
                elif fchild.type in {
                    "type_identifier", "integral_type", "floating_point_type",
                    "boolean_type", "void_type", "generic_type",
                    "scoped_type", "array_type"
                }:
                    f_type = fchild.text.decode('utf-8', errors='ignore')
                elif fchild.type == "variable_declarator":
                    for vchild in fchild.children:
                        if vchild.type == "identifier":
                            f_name = vchild.text.decode('utf-8', errors='ignore')
                        elif vchild.type not in {"=", "identifier"} and f_default is None:
                            f_default = vchild.text.decode('utf-8', errors='ignore')

            if f_name:
                fields.append({
                    "name": f_name,
                    "type_annotation": f_type,
                    "default_value": f_default,
                    "visibility": f_visibility,
                    "is_static": f_is_static
                })

    return fields


def _extract_class_methods(
    node: tree_sitter.Node,
    language: str,
    content: bytes
) -> List[str]:
    """
    Extract method names defined directly in a class (not nested classes).
    Returns a list of method name strings.
    """
    methods = []

    if language == "python":
        body = None
        for child in node.children:
            if child.type in {"block", "suite"}:
                body = child
                break
        if body:
            for stmt in body.children:
                target = stmt
                # Handle decorated_definition
                if stmt.type == "decorated_definition":
                    for dchild in stmt.children:
                        if dchild.type == "function_definition":
                            target = dchild
                            break

                if target.type == "function_definition":
                    for fchild in target.children:
                        if fchild.type == "identifier":
                            methods.append(fchild.text.decode('utf-8', errors='ignore'))
                            break

    elif language == "java":
        body = None
        for child in node.children:
            if child.type in {"class_body", "interface_body"}:
                body = child
                break
        if body:
            for stmt in body.children:
                if stmt.type == "method_declaration":
                    for mchild in stmt.children:
                        if mchild.type == "identifier":
                            methods.append(mchild.text.decode('utf-8', errors='ignore'))
                            break

    return methods


# --- QA Import Extraction ---


def _extract_imports(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str
) -> List[Dict[str, Any]]:
    """
    Extract import statements from a file.
    """
    imports = []

    def traverse(node: tree_sitter.Node):
        if language == "python":
            if node.type == "import_statement":
                imp = _parse_python_import(node, content)
                if imp:
                    imports.extend(imp)
            elif node.type == "import_from_statement":
                imp = _parse_python_import_from(node, content)
                if imp:
                    imports.append(imp)

        elif language == "java":
            if node.type == "import_declaration":
                imp = _parse_java_import(node, content)
                if imp:
                    imports.append(imp)

        # Only traverse top-level for imports
        for child in node.children:
            if child.type in {
                "module", "program", "compilation_unit",
                "import_statement", "import_from_statement", "import_declaration"
            }:
                traverse(child)

    traverse(tree.root_node)
    return imports


def _parse_python_import(
    node: tree_sitter.Node,
    content: bytes
) -> List[Dict[str, Any]]:
    """Parse `import X`, `import X as Y`, `import X, Y`."""
    results = []

    for child in node.children:
        if child.type == "dotted_name":
            module = child.text.decode('utf-8', errors='ignore')
            results.append({
                "module": module,
                "names": [],
                "alias": None,
                "is_wildcard": False
            })
        elif child.type == "aliased_import":
            module = None
            alias = None
            for achild in child.children:
                if achild.type == "dotted_name":
                    module = achild.text.decode('utf-8', errors='ignore')
                elif achild.type == "identifier":
                    alias = achild.text.decode('utf-8', errors='ignore')
            if module:
                results.append({
                    "module": module,
                    "names": [],
                    "alias": alias,
                    "is_wildcard": False
                })

    return results


def _parse_python_import_from(
    node: tree_sitter.Node,
    content: bytes
) -> Optional[Dict[str, Any]]:
    """Parse `from X import a, b` or `from X import *`."""
    module = None
    names = []
    is_wildcard = False
    seen_import_keyword = False

    for child in node.children:
        child_text = child.text.decode('utf-8', errors='ignore')

        # The 'import' keyword separates module part from imported names
        if child.type == "import":
            seen_import_keyword = True
            continue
        if child.type == "from":
            continue

        if not seen_import_keyword:
            # Before 'import' keyword → this is the module
            if child.type in {"dotted_name", "relative_import"}:
                module = child_text
        else:
            # After 'import' keyword → these are imported names
            if child.type == "wildcard_import":
                is_wildcard = True
            elif child.type == "dotted_name":
                names.append(child_text)
            elif child.type == "identifier":
                names.append(child_text)
            elif child.type == "aliased_import":
                for achild in child.children:
                    if achild.type in {"identifier", "dotted_name"}:
                        names.append(achild.text.decode('utf-8', errors='ignore'))
                        break  # just get the original name

    if not module:
        return None

    return {
        "module": module,
        "names": names,
        "alias": None,
        "is_wildcard": is_wildcard
    }


def _parse_java_import(
    node: tree_sitter.Node,
    content: bytes
) -> Optional[Dict[str, Any]]:
    """Parse Java import declaration."""
    import_text = node.text.decode('utf-8', errors='ignore').strip()

    # Remove 'import ' prefix and ';' suffix
    if import_text.startswith("import "):
        import_text = import_text[7:]
    if import_text.startswith("static "):
        import_text = import_text[7:]
    if import_text.endswith(";"):
        import_text = import_text[:-1]
    import_text = import_text.strip()

    is_wildcard = import_text.endswith(".*")
    if is_wildcard:
        module = import_text[:-2]
    else:
        # Split: com.example.ClassName → module=com.example, name=ClassName
        last_dot = import_text.rfind('.')
        if last_dot != -1:
            module = import_text[:last_dot]
            name = import_text[last_dot + 1:]
        else:
            module = import_text
            name = None

        return {
            "module": module,
            "names": [name] if name else [],
            "alias": None,
            "is_wildcard": False
        }

    return {
        "module": module,
        "names": [],
        "alias": None,
        "is_wildcard": True
    }


# --- Module-level Docstring ---


def _extract_module_doc(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str
) -> Optional[str]:
    """
    Extract module-level docstring.
    Python: first expression_statement in module containing a string literal.
    Java: first Javadoc comment at file level (before first class).
    """
    root = tree.root_node

    if language == "python":
        for child in root.children:
            if child.type == "expression_statement":
                string_node = _find_string_literal(child)
                if string_node:
                    text = content[string_node.start_byte:string_node.end_byte].decode(
                        'utf-8', errors='ignore'
                    )
                    return _clean_docstring_text(text)
                break  # Only check the very first statement
            elif child.type in {"import_statement", "import_from_statement", "comment"}:
                continue  # Skip imports and comments at top
            else:
                break  # First non-import/non-comment that isn't a docstring → no module doc

    elif language == "java":
        # Look for Javadoc comment before first class/interface
        for child in root.children:
            if child.type in {"class_declaration", "interface_declaration"}:
                javadoc = _find_javadoc_before_node(child, content)
                return javadoc
            elif child.type in {"import_declaration", "package_declaration"}:
                continue

    return None


# --- Slice-level QA parsing ---


def parse_slice_files_for_qa(
    repo_path: str,
    slice_commit_hash: str,
    config_extensions: Dict[str, List[str]],
    timeout_seconds: int = 30
) -> List[Dict[str, Any]]:
    """
    Parse all source files at a specific commit using QA-enriched extraction.
    Similar to parse_slice_files() but uses parse_file_for_qa().

    Args:
        repo_path: Path to repository
        slice_commit_hash: Commit hash to checkout
        config_extensions: Language to extension mapping
        timeout_seconds: Parsing timeout

    Returns:
        List of QA-enriched parsed file information
    """
    from git import Repo

    parsed_files = []

    try:
        repo = Repo(repo_path)

        if repo.head.is_detached:
            original_ref = repo.head.commit.hexsha
        else:
            original_ref = repo.active_branch.name

        repo.git.checkout(slice_commit_hash)

        source_files = []
        for ext_list in config_extensions.values():
            for ext in ext_list:
                source_files.extend(Path(repo_path).rglob(f"*{ext}"))

        for file_path in source_files:
            if any(part.startswith('.') for part in file_path.parts):
                continue

            language = detect_language(str(file_path), config_extensions)
            if not language:
                continue

            qa_data = parse_file_for_qa(str(file_path), language, timeout_seconds)
            if qa_data:
                parsed_files.append(qa_data)

        repo.git.checkout(original_ref)

    except Exception as e:
        logger.error(f"Error parsing slice files for QA: {e}")

    return parsed_files

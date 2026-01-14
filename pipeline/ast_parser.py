"""
Module: ast_parser.py

Purpose: Parse source code to extract symbol-level information only (functions, classes, comments).
Does NOT store full AST trees - only extracts symbols and discards the rest immediately.

Key Functions:
- parse_file(file_path: str, language: str) -> Optional[Dict]
  Returns symbol-level data: functions, classes, comments

Example:
    >>> result = parse_file("src/main.py", "python")
    >>> if result:
    ...     print(f"Functions: {len(result['functions'])}")
    ...     print(f"Classes: {len(result['classes'])}")
    ...     print(f"Comments: {len(result['comments'])}")
    5
"""

import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import tree_sitter
from tree_sitter import Language, Parser

logger = logging.getLogger(__name__)

# Tree-sitter language modules (will be loaded dynamically)
_languages = {}


def _should_traverse_node(node_type: str, language: str) -> bool:
    """
    Determine if a node type should be traversed (i.e., can legally contain symbols).
    Only module, class, and function nodes should be traversed.
    Do NOT traverse into statements or expressions.
    
    Args:
        node_type: Tree-sitter node type
        language: Programming language
        
    Returns:
        True if node can contain symbols and should be traversed
    """
    if language == "python":
        # Only traverse: module (root), class_definition, function_definition
        # Do NOT traverse: statements, expressions, assignments, etc.
        symbol_container_types = {
            "module",  # Root node
            "class_definition",
            "function_definition"
        }
        return node_type in symbol_container_types
    
    elif language == "java":
        # Only traverse: compilation_unit (root), class_declaration, interface_declaration, method_declaration
        # Do NOT traverse: statements, expressions, etc.
        symbol_container_types = {
            "compilation_unit",  # Root node
            "class_declaration",
            "interface_declaration",
            "method_declaration"
        }
        return node_type in symbol_container_types
    
    # Unknown language - be conservative and only traverse known symbol containers
    return False


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
    Parse a source code file and extract symbol-level information only.
    All AST traversal and extraction happens inside this function.
    Does NOT return tree or content - only symbol-level data compatible with slice schema.
    
    Args:
        file_path: Path to source file
        language: Programming language
        timeout_seconds: Timeout for parsing
        
    Returns:
        Dictionary containing symbol-level data:
        {
            "file_path": str,
            "content_hash": str,
            "language": str,
            "functions": List[Dict],
            "classes": List[Dict],
            "comments": List[Dict]
        }
        Returns None if error
    """
    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.warning(f"File not found: {file_path}")
            return None
        
        # Read file content
        with open(file_path, 'rb') as f:
            content = f.read()
        
        if not content:
            return None
        
        # Load language
        lang = _load_language(language)
        if not lang:
            return None
        
        # Create parser
        parser = Parser(lang)
        
        # Parse (tree and content are used only temporarily for extraction)
        tree = parser.parse(content)
        
        # Extract all symbol-level data
        # All extraction happens here - tree and content are discarded after
        functions = _extract_functions_internal(tree, content, language, file_path)
        classes = _extract_classes_internal(tree, content, language, file_path)
        comments = _extract_comments_internal(tree, content, language, file_path)
        
        # Calculate content hash
        content_hash = calculate_content_hash(content)
        
        # Return only symbol-level data (no tree, no content)
        return {
            "file_path": file_path,
            "content_hash": content_hash,
            "language": language,
            "functions": functions,
            "classes": classes,
            "comments": comments
        }
        
    except Exception as e:
        logger.warning(f"Error parsing file {file_path}: {e}")
        return None


def _extract_functions_internal(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str,
    file_path: str
) -> List[Dict[str, Any]]:
    """
    Internal function to extract function definitions from AST (symbol-level only).
    Called by parse_file during parsing.
    
    Args:
        tree: Parsed tree-sitter tree
        content: Source code bytes
        language: Programming language
        
    Returns:
        List of function information dictionaries with symbol-level schema
    """
    functions = []
    
    # Language-specific function node types
    function_types = {
        "python": ["function_definition"],
        "java": ["method_declaration"]
    }
    
    target_types = function_types.get(language, [])
    
    def traverse(node: tree_sitter.Node, container: Optional[str] = None):
        """
        Shallow, type-constrained traversal.
        Only descend into nodes that can legally contain symbols (module → class → function).
        Do not traverse into statements or expressions.
        """
        # Extract function if this is a function node
        if node.type in target_types:
            func_info = _extract_function_symbol(node, language, content, container, file_path)
            if func_info:
                functions.append(func_info)
        
        # Determine container for nested functions (methods in classes)
        current_container = container
        if language == "python" and node.type == "class_definition":
            # Extract class name for container
            for child in node.children:
                if child.type == "identifier":
                    current_container = child.text.decode('utf-8', errors='ignore')
                    break
        elif language == "java" and node.type in ["class_declaration", "interface_declaration"]:
            for child in node.children:
                if child.type == "type_identifier":
                    current_container = child.text.decode('utf-8', errors='ignore')
                    break
        
        # Only traverse children that can contain symbols
        # Skip statements, expressions, and other non-symbol containers
        for child in node.children:
            if _should_traverse_node(child.type, language):
                traverse(child, current_container)
    
    traverse(tree.root_node)
    return functions


def _extract_function_symbol(
    node: tree_sitter.Node,
    language: str,
    content: bytes,
    container: Optional[str],
    file_path: str
) -> Optional[Dict[str, Any]]:
    """
    Extract symbol-level information from a function/method node.
    
    Returns:
        Function symbol dict aligned to schema: name, kind, container, signature,
        parameters, return_type, visibility, is_static, start_line, end_line, doc, file
    """
    # Extract name
    name = None
    for child in node.children:
        if language == "python" and child.type == "identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break
        elif language == "java" and child.type == "identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break
    
    if not name:
        return None
    
    # Determine kind
    if container:
        kind = "method"
        if language == "java" and name == container:
            kind = "constructor"
    else:
        kind = "function"
    
    # Extract signature (first line of declaration)
    start_line = node.start_point[0] + 1  # 1-indexed
    end_line = node.end_point[0] + 1
    start_byte = node.start_byte
    # Find first newline after start
    first_line_end = content.find(b'\n', start_byte)
    if first_line_end == -1 or first_line_end > node.end_byte:
        first_line_end = node.end_byte
    signature = content[start_byte:first_line_end].decode('utf-8', errors='ignore').strip()
    
    # Extract parameters (parameter names)
    parameters = []
    if language == "python":
        for child in node.children:
            if child.type == "parameters":
                for param in child.children:
                    if param.type == "identifier":
                        param_name = param.text.decode('utf-8', errors='ignore')
                        parameters.append(param_name)
                    elif param.type == "typed_parameter":
                        for pchild in param.children:
                            if pchild.type == "identifier":
                                param_name = pchild.text.decode('utf-8', errors='ignore')
                                parameters.append(param_name)
                                break
    elif language == "java":
        for child in node.children:
            if child.type == "formal_parameters":
                for param in child.children:
                    if param.type == "formal_parameter":
                        for pchild in param.children:
                            if pchild.type == "identifier":
                                param_name = pchild.text.decode('utf-8', errors='ignore')
                                parameters.append(param_name)
                                break
    
    # Extract return type (Java only, Python uses type hints if available)
    return_type = None
    if language == "java":
        for child in node.children:
            if child.type == "type_identifier" or child.type == "void":
                return_type = child.text.decode('utf-8', errors='ignore')
                break
    
    # Extract visibility and static (Java only)
    visibility = None
    is_static = False
    if language == "java":
        for child in node.children:
            if child.type in ["public", "private", "protected"]:
                visibility = child.type
            elif child.type == "static":
                is_static = True
    
    # Documentation comments are extracted separately in extract_comments()
    # This field is kept for compatibility but returns None
    doc = None
    
    return {
        "name": name,
        "kind": kind,
        "container": container,
        "signature": signature,
        "parameters": parameters,
        "return_type": return_type,
        "visibility": visibility,
        "is_static": is_static,
        "start_line": start_line,
        "end_line": end_line,
        "doc": doc,
        "file": file_path
    }


def _extract_classes_internal(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str,
    file_path: str
) -> List[Dict[str, Any]]:
    """
    Internal function to extract class definitions from AST (symbol-level only).
    Called by parse_file during parsing.
    
    Args:
        tree: Parsed tree-sitter tree
        content: Source code bytes
        language: Programming language
        
    Returns:
        List of class information dictionaries with symbol-level schema
    """
    classes = []
    
    # Language-specific class node types
    class_types = {
        "python": ["class_definition"],
        "java": ["class_declaration", "interface_declaration"]
    }
    
    target_types = class_types.get(language, [])
    
    def traverse(node: tree_sitter.Node):
        """
        Shallow, type-constrained traversal.
        Only descend into nodes that can legally contain symbols (module → class → function).
        Do not traverse into statements or expressions.
        """
        # Extract class if this is a class node
        if node.type in target_types:
            class_info = _extract_class_symbol(node, language, content, file_path)
            if class_info:
                classes.append(class_info)
        
        # Only traverse children that can contain symbols
        # Skip statements, expressions, and other non-symbol containers
        for child in node.children:
            if _should_traverse_node(child.type, language):
                traverse(child)
    
    traverse(tree.root_node)
    return classes


def _extract_class_symbol(
    node: tree_sitter.Node,
    language: str,
    content: bytes,
    file_path: str
) -> Optional[Dict[str, Any]]:
    """
    Extract symbol-level information from a class/interface node.
    
    Returns:
        Class symbol dict aligned to schema: name, kind, base_classes,
        implemented_interfaces, start_line, end_line, doc, file
    """
    # Extract name
    name = None
    for child in node.children:
        if language == "python" and child.type == "identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break
        elif language == "java" and child.type == "type_identifier":
            name = child.text.decode('utf-8', errors='ignore')
            break
    
    if not name:
        return None
    
    # Determine kind
    if language == "java" and node.type == "interface_declaration":
        kind = "interface"
    else:
        kind = "class"
    
    # Extract base classes / extended classes
    base_classes = []
    implemented_interfaces = []
    
    if language == "python":
        for child in node.children:
            if child.type == "argument_list":  # Python inheritance in parentheses
                for arg in child.children:
                    if arg.type == "identifier":
                        base_classes.append(arg.text.decode('utf-8', errors='ignore'))
    elif language == "java":
        for child in node.children:
            if child.type == "superclass":
                for schild in child.children:
                    if schild.type == "type_identifier":
                        base_classes.append(schild.text.decode('utf-8', errors='ignore'))
                        break
            elif child.type == "super_interfaces":
                for iface in child.children:
                    if iface.type == "type_identifier":
                        implemented_interfaces.append(iface.text.decode('utf-8', errors='ignore'))
    
    start_line = node.start_point[0] + 1  # 1-indexed
    end_line = node.end_point[0] + 1
    
    # Documentation comments are extracted separately in extract_comments()
    # This field is kept for compatibility but returns None
    doc = None
    
    return {
        "name": name,
        "kind": kind,
        "base_classes": base_classes,
        "implemented_interfaces": implemented_interfaces,
        "start_line": start_line,
        "end_line": end_line,
        "doc": doc,
        "file": file_path
    }


def _extract_comments_internal(
    tree: tree_sitter.Tree,
    content: bytes,
    language: str,
    file_path: str
) -> List[Dict[str, Any]]:
    """
    Internal function to extract documentation comments only (Python docstrings and Java Javadoc).
    Discard inline and block comments not associated with a symbol.
    Called by parse_file during parsing.
    
    Args:
        tree: Parsed tree-sitter tree
        content: Source code bytes
        language: Programming language
        file_path: Path to file (for owner identification)
        
    Returns:
        List of documentation comment information dictionaries
    """
    comments = []
    
    if language == "python":
        # Extract docstrings from functions and classes
        comments = _extract_python_docstrings(tree, content, file_path)
    elif language == "java":
        # Extract Javadoc comments from classes and methods
        comments = _extract_java_javadoc(tree, content, file_path)
    else:
        # Unknown language - return empty list
        return []
    
    return comments


def _extract_python_docstrings(tree: tree_sitter.Tree, content: bytes, file_path: str) -> List[Dict[str, Any]]:
    """
    Extract Python docstrings from functions and classes.
    Docstrings are string literals that appear as the first statement in a function/class.
    
    Returns:
        List of docstring comment dictionaries
    """
    docstrings = []
    
    def traverse(node: tree_sitter.Node, container_type: Optional[str] = None, container_name: Optional[str] = None):
        """
        Traverse symbol containers to find docstrings.
        """
        # Check if this is a function or class definition
        current_container_type = container_type
        current_container_name = container_name
        
        if node.type == "function_definition":
            # Extract function name
            func_name = None
            for child in node.children:
                if child.type == "identifier":
                    func_name = child.text.decode('utf-8', errors='ignore')
                    break
            
            if func_name:
                # Look for docstring (first string literal in function body)
                docstring = _find_docstring_in_node(node, content)
                if docstring:
                    docstrings.append({
                        "kind": "docstring",
                        "owner_type": "function",
                        "owner_name": func_name,
                        "text": docstring,
                        "start_line": node.start_point[0] + 1,
                        "end_line": node.end_point[0] + 1,
                        "file": file_path
                    })
            
            current_container_type = "function"
            current_container_name = func_name
        
        elif node.type == "class_definition":
            # Extract class name
            class_name = None
            for child in node.children:
                if child.type == "identifier":
                    class_name = child.text.decode('utf-8', errors='ignore')
                    break
            
            if class_name:
                # Look for docstring (first string literal in class body)
                docstring = _find_docstring_in_node(node, content)
                if docstring:
                    docstrings.append({
                        "kind": "docstring",
                        "owner_type": "class",
                        "owner_name": class_name,
                        "text": docstring,
                        "start_line": node.start_point[0] + 1,
                        "end_line": node.end_point[0] + 1,
                        "file": file_path
                    })
            
            current_container_type = "class"
            current_container_name = class_name
        
        # Only traverse symbol containers
        for child in node.children:
            if _should_traverse_node(child.type, "python"):
                traverse(child, current_container_type, current_container_name)
    
    traverse(tree.root_node)
    return docstrings


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


def _extract_java_javadoc(tree: tree_sitter.Tree, content: bytes, file_path: str) -> List[Dict[str, Any]]:
    """
    Extract Java Javadoc comments from classes and methods.
    Javadoc comments start with /** and must be associated with a symbol.
    
    Returns:
        List of Javadoc comment dictionaries
    """
    javadocs = []
    
    def traverse(node: tree_sitter.Node, container_type: Optional[str] = None, container_name: Optional[str] = None):
        """
        Traverse symbol containers to find Javadoc comments.
        """
        current_container_type = container_type
        current_container_name = container_name
        
        # Check for Javadoc comment immediately before this node
        javadoc = _find_javadoc_before_node(node, content)
        
        if node.type == "method_declaration":
            # Extract method name
            method_name = None
            for child in node.children:
                if child.type == "identifier":
                    method_name = child.text.decode('utf-8', errors='ignore')
                    break
            
            if method_name and javadoc:
                javadocs.append({
                    "kind": "javadoc",
                    "owner_type": "function",
                    "owner_name": method_name,
                    "text": javadoc,
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "file": file_path
                })
            
            current_container_type = "function"
            current_container_name = method_name
        
        elif node.type in ["class_declaration", "interface_declaration"]:
            # Extract class/interface name
            class_name = None
            for child in node.children:
                if child.type == "type_identifier":
                    class_name = child.text.decode('utf-8', errors='ignore')
                    break
            
            if class_name and javadoc:
                javadocs.append({
                    "kind": "javadoc",
                    "owner_type": "class",
                    "owner_name": class_name,
                    "text": javadoc,
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "file": file_path
                })
            
            current_container_type = "class"
            current_container_name = class_name
        
        # Only traverse symbol containers
        for child in node.children:
            if _should_traverse_node(child.type, "java"):
                traverse(child, current_container_type, current_container_name)
    
    traverse(tree.root_node)
    return javadocs


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
    
    Args:
        repo_path: Path to repository
        slice_commit_hash: Commit hash to checkout
        config_extensions: Language to extension mapping
        timeout_seconds: Parsing timeout
        
    Returns:
        List of parsed file information
    """
    from git import Repo
    
    parsed_files = []
    
    try:
        repo = Repo(repo_path)
        
        # Checkout the commit
        repo.git.checkout(slice_commit_hash)
        
        # Find all source files
        source_files = []
        for ext_list in config_extensions.values():
            for ext in ext_list:
                source_files.extend(Path(repo_path).rglob(f"*{ext}"))
        
        # Parse each file
        for file_path in source_files:
            # Skip hidden files and common exclusions
            if any(part.startswith('.') for part in file_path.parts):
                continue
            
            language = detect_language(str(file_path), config_extensions)
            if not language:
                continue
            
            ast_data = parse_file(str(file_path), language, timeout_seconds)
            if ast_data:
                parsed_files.append(ast_data)
        
        # Return to original branch
        repo.git.checkout('-')
        
    except Exception as e:
        logger.error(f"Error parsing slice files: {e}")
    
    return parsed_files

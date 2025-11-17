import inspect
import ast


def extract_imports_from_source(logger, fn):
    """
    Inspect the function source code and extract top-level imports.
    """
    pkgs = set()
    try:
        source = inspect.getsource(fn)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    pkgs.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    pkgs.add(node.module.split(".")[0])
    except OSError:
        if logger:
            logger.warn(fn.__name__ + " is a builtin/C extension (explicit deps only)")
    return pkgs

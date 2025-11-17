import functools
import os
from ml3.runner.extract_imports import extract_imports_from_source
from ml3.runner.create_environment import create_environment_file
from ml3.runner.run_in_env import run_in_venv


def env(packages=(), logger=None, timeout=0):
    def decorator(fn):
        explicit = set(packages)
        inferred = extract_imports_from_source(logger, fn) - explicit
        inferred.add("cloudpickle")  # daemon requires this
        fn._dependencies = explicit | inferred

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            related = {fn}
            for a in args:
                if hasattr(a, "_dependencies"):
                    related.add(a)
            for a in kwargs.values():
                if hasattr(a, "_dependencies"):
                    related.add(a)
            combined = set(fn._dependencies)
            for r in related:
                combined |= r._dependencies
            if os.environ.get("ML3_IN_DAEMON") == "1":
                return fn(*args, **kwargs)
            _, venv_name = create_environment_file(logger, related, combined)
            return run_in_venv(venv_name, fn, *args, timeout=timeout, **kwargs)
        return wrapper
    return decorator

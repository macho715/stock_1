
import importlib.util
from types import ModuleType
from pathlib import Path

def run_invoice_validation_as_module(invoice_py_path: str) -> None:
    p = Path(invoice_py_path)
    if not p.exists():
        raise FileNotFoundError(f"Invoice script not found: {p}")
    spec = importlib.util.spec_from_file_location("invoice_mod", p)
    mod = importlib.util.module_from_spec(spec)  # type: ModuleType
    spec.loader.exec_module(mod)

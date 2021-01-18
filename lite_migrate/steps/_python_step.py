from typing import Callable
from ._base_step import BaseStep

__all__ = ["PythonStep"]


class PythonStep(BaseStep):
    def __init__(self, callable_: Callable):
        self._callable = callable_

    def __call__(self, *args, **kwargs):
        assert kwargs.get("cur")
        self._callable(*args, **kwargs)

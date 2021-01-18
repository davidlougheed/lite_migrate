from abc import ABC, abstractmethod

__all__ = ["BaseStep"]


class BaseStep(ABC):
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

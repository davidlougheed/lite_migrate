from ._base_step import BaseStep

__all__ = ["SQLStep"]


class SQLStep(BaseStep):
    def __init__(self, sql: str):
        self._sql = sql

    def __call__(self, *args, **kwargs):
        cur = kwargs.get("cur")
        assert cur
        cur.execute(self._sql)

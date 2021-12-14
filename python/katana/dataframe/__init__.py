from abc import ABC, abstractmethod
from collections.abc import Sized
from typing import Any, Callable, Dict, Optional, Sequence, Union

import numpy
import pandas
import pyarrow

from . import abc


class DataFrame(ABC, abc.ReadOnlyDataFrame):
    """
    A data-frame interface to Katana data.
    """

    def __repr__(self):
        if len(self) > 1:
            cols = "\t".join(f"{k}: {t}" for k, t in self.dtypes.items())
            return f"Katana DataFrame\n{cols}\n(length: {len(self)})"
        return "\t".join(f"{k}: {self.at[0, k]}" for k in self.columns)


class LazyDataAccessor(ABC):
    @abstractmethod
    def __getitem__(self, i):
        raise NotImplementedError()

    @abstractmethod
    def array(self, items: slice):
        raise NotImplementedError()


class LazyDataFrame(DataFrame):
    """
    A data-frame that computes its data lazily based on a set of collections and `LazyDataAccessor`.
    """

    def __init__(
        self,
        data: Dict[str, Union[Callable, Sequence]],
        dtypes: Sequence[Any],
        *,
        length: Optional[int] = None,
        offset: int = 0,
        stride=1,
    ):
        self._data = data
        self._dtypes = dtypes
        self._offset = offset
        self._stride = stride
        if length is not None:
            self._length = length
        else:
            # if we don't have a specified length, look for one.
            for col in data.values():
                if isinstance(col, Sized):
                    self._length = len(col)
                    break
            else:
                raise TypeError("No data values have a length, so length must be provided as an argument.")

    def _get_rows(self, item: slice) -> "DataFrame":
        start, stop, stride = item.indices(len(self))
        return LazyDataFrame(
            self._data,
            self._dtypes,
            length=((stop - 1) - start) // stride + 1,
            offset=self._offset + start * stride,
            stride=self._stride * stride,
        )

    def _get_columns(self, item: Sequence[str]) -> "DataFrame":
        for k in item:
            if k not in self._data:
                raise ValueError(k)
        data = {k: v for k, v in self._data.items() if k in item}
        dtypes = [self._dtypes[i] for i, k in enumerate(self._data.keys()) if k in item]
        return LazyDataFrame(data, dtypes, length=self._length, offset=self._offset, stride=self._stride)

    def _get_cell(self, row: int, col):
        row = row * self._stride + self._offset
        col_data = self._data[col]
        if isinstance(col_data, (numpy.ndarray, pandas.Series, pyarrow.Array, range, Sequence, LazyDataAccessor)):
            return col_data[row]
        return col_data

    def __len__(self) -> int:
        return self._length

    def _get_column(self, item: str):
        col_data = self._data[item]
        indexes = slice(self._offset, self._offset + self._length, self._stride)
        if isinstance(col_data, LazyDataAccessor):
            col_data = col_data.array(indexes)
        if isinstance(col_data, (numpy.ndarray, pandas.Series, range)):
            return col_data
        if isinstance(col_data, (pyarrow.Array,)):
            return col_data.to_pandas()
        data = numpy.empty(len(self), dtype=self.dtypes[item])
        data[:] = col_data
        return data

    def to_pandas(self) -> pandas.DataFrame:
        data = {}
        for col_name in self.columns:
            data[col_name] = self._get_column(col_name)
        return pandas.DataFrame(data)

    @property
    def dtypes(self) -> dict:
        return dict(zip(self._data.keys(), self._dtypes))

    @property
    def columns(self):
        return self._data.keys()

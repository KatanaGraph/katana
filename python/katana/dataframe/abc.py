import collections.abc
from abc import ABC, abstractmethod
from typing import Any, Sequence, Union

import pandas


class _AtIndexer:
    def __init__(self, underlying):
        self._underlying = underlying

    def __getitem__(self, item):
        return self._underlying._get_cell(*item)


class ReadOnlyDataFrame(collections.abc.Sequence):
    @property
    @abstractmethod
    def dtypes(self) -> dict:
        """
        :return: The dtype of each column as a dict mapping column names to dtypes.
        """
        return {}

    @abstractmethod
    def _get_rows(self, item: slice):
        """
        :param item: A slice selecting a set of rows.
        :return: A view on the rows selected by item,
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_columns(self, item: Sequence[str]):
        """
        :param item: A sequence of column names.
        :return: A view on the columns selected by item,
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_column(self, item: str):
        """
        :param item: A column name.
        :return: A sequence of the columns data.
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_cell(self, row: int, col):
        """
        :return: Get the single value in row ``row`` and column ``col``.
        """
        raise NotImplementedError()

    def __getitem__(self, item: Union[Any, Sequence[Any], slice]):
        """
        Get a view on a column, set of columns, or slice of rows.

        :param item: The elements to select from this dataframe.
        """
        if isinstance(item, slice):
            return self._get_rows(item)
        if isinstance(item, Sequence) and not isinstance(item, str):
            return self._get_columns(item)
        return self._get_column(item)

    def __getattr__(self, item):
        if item in self.columns:
            return self[item]
        raise AttributeError(item)

    def __iter__(self):
        return (self._get_rows(slice(i, i + 1)) for i in range(len(self)))

    @abstractmethod
    def to_pandas(self) -> pandas.DataFrame:
        """
        Create a pandas dataframe from this dataframe by copying the data.

        :return: A new `pandas.DataFrame` containing the data from this frame.
        """
        raise NotImplementedError()

    @property
    def shape(self):
        return len(self), len(self.columns)

    @property
    def at(self):
        """
        >> df.at[row, column]

        Index a single element from the dataframe.
        """
        return _AtIndexer(self)

    @property
    @abstractmethod
    def columns(self):
        """
        :return: a sequence containing all the column names in this dataframe.
        """

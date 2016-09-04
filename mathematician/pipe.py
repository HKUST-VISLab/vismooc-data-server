# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class PipeModule(metaclass=ABCMeta):
    """An abstract class. All processing module who should be past
    to a pipline need to inherient
    """

    def __init__(self):
        pass

    @abstractmethod
    def process(self, raw_data):
        """Required.Process the raw data and return the processed data

        Args:
            raw_data (dict): The first parameter.

        Returns:
            dict: The return value, processed data.
        """
        processed_data = raw_data
        return processed_data


class Pipe:
    """A pipeline class, which is used to schedule the pipeline of
    data processing.

    Attributes:
        __raw_data (dict): raw data need to be processed.
        __processed_data (dict): data after processing.
    """

    def __init__(self):
        self.__raw_data = None
        self._processed_data = None

    def input(self, data):
        """
            input raw data for processing
        """
        # if type(data) is not dict:
        #     return None
        self.__raw_data = data
        # in the beginning both raw data and processed data are the same
        self._processed_data = data
        return self

    def pipe(self, processor):
        """
            Chain the module one by one so that they will run sequentially
        """
        if isinstance(processor, PipeModule) is False and self._processed_data is None:
            return None

        self._processed_data = processor.process(self._processed_data)
        return self

    def output(self):
        return self._processed_data

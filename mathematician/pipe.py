# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from datetime import datetime


class PipeModule(metaclass=ABCMeta):
    """An abstract class. All processing module who should be past
    to a pipline need to inherient
    """

    def __init__(self):
        pass

    @abstractmethod
    def process(self, raw_data, raw_data_files = None):
        """Required.Process the raw data and return the processed data

        Args:
            raw_data (dict): The first parameter.

        Returns:
            dict: The return value, processed data.
        """
        pass


class Pipe:
    """A pipeline class, which is used to schedule the pipeline of
    data processing.

    Attributes:
        __raw_data (dict): raw data need to be processed.
        __processed_data (dict): data after processing.
    """

    def __init__(self):
        self.__raw_data_files = []
        # self.__raw_data = None
        self._processed_data = None
        self._processors = []

    # def input(self, data):
    #     """
    #         input raw data for processing
    #     """

    #     self.__raw_data = {
    #         "created_date": datetime.now(),
    #         "data": data
    #     }
    #     # in the beginning both raw data and processed data are the same
    #     self._processed_data = self.__raw_data
    #     return self

    def input_file(self, filename):
        if type(filename) is not str or len(filename) == 0:
            raise TypeError('filename should be a non-empty str')
        self.__raw_data_files.append(filename)
        return self

    def input_files(self, filenames):
        if type(filenames) is not list:
            raise TypeError('filenames should be a non-empty list')
        self.__raw_data_files += filenames
        return self

    def pipe(self, processor):
        """
            Chain the module one by one so that they will run sequentially
        """
        if isinstance(processor, PipeModule) is False:
            return None
        self._processors.append(processor)
        return self

    def concat(self, pipeline):
        """
            Concatenate another pipeline to this pipeline
        """
        return self

    def output(self):
        """
            Excute all processor one by one and return the data after processing
        """
        if self._processed_data is not None:
            self._processed_data = {'created_date': datetime.now(), 'data': {}}

            for processor in self._processors:
                self._processed_data = processor.process(self._processed_data, self.__raw_data_files)

                self._processed_data['finished_date'] = datetime.now()
        return self._processed_data

'''Unit test for pipe module
'''
# pylint: disable=C0111, C0103
import unittest
from mathematician.pipe import PipeLine, PipeModule


class MoocProcessor(PipeModule):

    def __init__(self):
        super().__init__()

    def process(self, raw_data, raw_data_filenames=None):
        raw_data = {"data": raw_data_filenames[0]}
        return raw_data


class TestPipeClass(unittest.TestCase):
    '''Unit test for pipe module
    '''

    def setUp(self):
        self.pipeline = PipeLine()

    def test_input_file_with_right_params(self):
        self.assertIs(self.pipeline.input_file("It's a file"), self.pipeline,
                      'when input a filename, pipe should return itself')

    def test_input_file_with_wrong_params(self):
        self.assertIsNone(self.pipeline.input_file(None),
                          'when input is empty, pipe should return none')
        self.assertIsNone(self.pipeline.input_file(1),
                          'when input is not dict type, pipe should return none')

    def test_pipe_with_right_params(self):
        processor = MoocProcessor()
        test_input = "a file"
        self.assertIs(self.pipeline.input_file(test_input).pipe(processor), self.pipeline,
                      'when pipe a instance of PipeModule, pipe should return itself')
        self.assertIs(self.pipeline.output()["data"], test_input,
                      'should return raw data if processor do nothing')

    def test_pipe_with_wrong_params(self):
        self.assertIsNone(self.pipeline.pipe({}),
                          'when pipe a none-PipeModule instance, pipe will return None')
        processor = MoocProcessor()
        self.assertIsNone(self.pipeline.pipe(processor),
                          'when no input data, pipe will return None')

    def test_outout(self):
        self.assertIsInstance(self.pipeline.output()["created_date"], str,
                              'when no input data, output will return a object with property \
                              created_date')
        self.assertIsInstance(self.pipeline.output()["data"], dict,
                              'when no input data, output will return a object with property data')
        test_input = "a file"
        processor = MoocProcessor()
        self.assertIs(self.pipeline.input_file(test_input).pipe(processor).output()["data"],
                      test_input, 'when data has been input, output will return processed_data')

import unittest
from mathematician.pipe import PipeLine


class TestPipeClass(unittest.TestCase):

    def setUp(self):
        self.pipeline = PipeLine()

    def test_input_with_right_params(self):
        self.assertIs(self.pipeline.input_file("It's a file"), self.pipeline,
                      'when input a object, pipe should return itself')

    def test_input_with_wrong_params(self):
        self.assertIsNone(self.pipeline.input(None),
                          'when input is empty, pipe should return none')
        self.assertIsNone(self.pipeline.input(1),
                          'when input is not dict type, pipe should return none')

    def test_pipe_with_right_params(self):
        processor = pipe.PipeModule()
        test_input = {'a': 10}
        self.assertIs(self.pipeline.input(test_input).pipe(processor), self.pipeline,
                      'when pipe a instance of PipeModule, pipe should return itself')
        self.assertIs(self.pipeline.output(), test_input,
                      'should return raw data if processor do nothing')

    def test_pipe_with_wrong_params(self):
        self.assertIsNone(self.pipeline.pipe({}),
                          'when pipe a none-PipeModule instance, pipe will return None')
        processor = pipe.PipeModule()
        self.assertIsNone(self.pipeline.pipe(processor),
                          'when no input data, pipe will return None')

    def test_outout(self):
        self.assertIsNone(self.pipeline.output(),
                          'when no input data, output will return None')
        test_input = {'a': 10}
        self.assertIs(self.pipeline.input(test_input).output(), test_input,
                      'when data has been input, output will return processed_data')

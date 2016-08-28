import unittest
from mathematician import pipe


class TestPipeClass(unittest.TestCase):

    def setUp(self):
        self.pipe = pipe.Pipe()

    def test_input_with_right_params(self):
        self.assertIs(self.pipe.input({}), self.pipe,
                      'when input a object, pipe should return itself')

    def test_input_with_wrong_params(self):
        self.assertIsNone(self.pipe.input(None),
                          'when input is empty, pipe should return none')
        self.assertIsNone(self.pipe.input(1),
                          'when input is not dict type, pipe should return none')

    def test_pipe_with_right_params(self):
        processor = pipe.PipeModule()
        test_input = {'a': 10}
        self.assertIs(self.pipe.input(test_input).pipe(processor), self.pipe,
                      'when pipe a instance of PipeModule, pipe should return itself')
        self.assertIs(self.pipe.output(), test_input,
                      'should return raw data if processor do nothing')

    def test_pipe_with_wrong_params(self):
        self.assertIsNone(self.pipe.pipe({}),
                          'when pipe a none-PipeModule instance, pipe will return None')
        processor = pipe.PipeModule()
        self.assertIsNone(self.pipe.pipe(processor),
                          'when no input data, pipe will return None')

    def test_outout(self):
        test_input = {'a': 10}
        self.assertIs(self.pipe.input(test_input).output(), test_input,
                      'output will return processed_data')
        self.assertIsNone(self.pipe.output(),
                          'when no input data, output will return None')

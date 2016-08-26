import unittest
from mathematician import pipe

class TestPipeClass(unittest.TestCase):

    def setUp(self):
        self.pipe = pipe.Pipe()

    def test_input_with_normal_params(self):
        self.assertIs(self.pipe.input({}), self.pipe,
                      'when input a object, pipe should return itself')

    def test_input_with_wrong_params(self):
        self.assertIsNone(self.pipe.input(None),
                          'when input is empty, pipe should return none')
        self.assertIsNone(self.pipe.input(1),
                          'when input is not dict type, pipe should return none')

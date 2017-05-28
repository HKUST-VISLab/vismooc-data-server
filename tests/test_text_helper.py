'''Unit test for text_helper module
'''
# pylint: disable=C0111, C0103
import unittest
from mathematician import text_helper
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class SentimentAnalyzer(unittest.TestCase):

    def test_constructor(self):
        sentiment_analyzor = text_helper.SentimentAnalyzer()
        self.assertIsInstance(sentiment_analyzor.analyzer, SentimentIntensityAnalyzer)

    def test_analysis(self):
        sentiment_analyzor = text_helper.SentimentAnalyzer()
        result = sentiment_analyzor.analysis('good')
        # just for hack
        self.assertIsNotNone(result)

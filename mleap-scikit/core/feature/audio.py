from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np


class ZeroCrossingRate(BaseEstimator, TransformerMixin):
    def __init__(self, midterm_window, midterm_step, shortterm_window, shortterm_step):
        self.midterm_window = midterm_window
        self.midterm_step = midterm_step
        self.shortterm_window = shortterm_window
        self.shortterm_step = shortterm_step
        self.midterm_window_ratio = int(round(midterm_window / shortterm_step)) ## TODO: Confirm this should be midterm_window / midterm_step
        self.shortterm_window_ratio = int(round(shortterm_window / shortterm_step))


    def fit(self, mono_input):
        """

        :param mono_input:
        :return:
        """
        signal = np.double(mono_input)
from . import *


def _check_numpy_unicode_bug(labels):
    """Check that user is not subject to an old numpy bug

    Fixed in master before 1.7.0:

      https://github.com/numpy/numpy/pull/243

    """
    if np_version[:3] < (1, 7, 0) and labels.dtype.kind == 'U':
        raise RuntimeError("NumPy < 1.7.0 does not implement searchsorted"
                           " on unicode data correctly. Please upgrade"
                           " NumPy to use LabelEncoder with unicode inputs.")


class LabelEncoder(LabelEncoder):
    def __init__(self, feature):
        self.feature = feature
        self.classes_ = []

    def fit(self, df):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        y = column_or_1d(df[self.feature], warn=True)
        _check_numpy_unicode_bug(y)
        self.classes_ = np.unique(y)
        return self

    def fit_transform(self, df, y=None):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        y = column_or_1d(df[self.feature], warn=True)
        _check_numpy_unicode_bug(y)
        self.classes_, y = np.unique(y, return_inverse=True)
        return y

    def transform(self, df, y=None):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        check_is_fitted(self, 'classes_')

        classes = np.unique(df[self.feature])
        _check_numpy_unicode_bug(classes)
        if len(np.intersect1d(classes, self.classes_)) < len(classes):
            diff = np.setdiff1d(classes, self.classes_)
            raise ValueError("y contains new labels: %s" % str(diff))
        return np.searchsorted(self.classes_, df[self.feature])
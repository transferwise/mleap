from . import *
from sklearn.preprocessing.data import _transform_selected


class OneHotEncoder(OneHotEncoder):
    def __init__(self, feature):
        self.feature = feature

    def fit(self, df, y):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        self.fit_transform(df[[self.feature]], None)
        return self

    def fit_transform(self, df, y):
        """Fit OneHotEncoder to X, then transform X.

        Equivalent to self.fit(X).transform(X), but more convenient and more
        efficient. See fit for the parameters, transform for the return value.
        """
        return _transform_selected(df[[self.feature]], self._fit_transform,
                                   self.categorical_features, copy=True)

    def transform(self, df, y):
        """
        :type df: pd.DataFrame
        :param df:
        :param y:
        :return:
        """
        return _transform_selected(df[[self.feature]], self._transform,
                                   self.categorical_features, copy=True)
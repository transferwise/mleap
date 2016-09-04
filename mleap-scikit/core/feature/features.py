from . import *
from sklearn.preprocessing.data import _transform_selected
from sklearn.preprocessing.data import BaseEstimator, TransformerMixin, StandardScaler
from mleap.core.utils.transformer import uid
import numpy as np


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
    def __init__(self, input_feature, output_feature=None):
        self.op='string_indexer'
        self.name = uid(self.op)
        self.input_feature = input_feature
        self._output_feature = output_feature
        self.output_feature = self._set_output_feature()
        self.classes_ = []

    def _set_output_feature(self):
        if pd.isnull(self._output_feature):
            return self.input_feature
        return self._output_feature

    def fit(self, df):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        y = column_or_1d(df[self.input_feature], warn=True)
        _check_numpy_unicode_bug(y)
        self.classes_ = np.unique(y)
        return self

    def fit_transform(self, df, y=None, **fit_params):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        Xt = pd.DataFrame(df)
        y = column_or_1d(Xt[self.input_feature], warn=True)
        _check_numpy_unicode_bug(y)
        self.classes_, y = np.unique(y, return_inverse=True)

        Xt[self.output_feature] = y
        return Xt

    def transform(self, df, y=None):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        check_is_fitted(self, 'classes_')
        Xt = pd.DataFrame(df)
        if y is None:
            y = self.input_feature
        classes = np.unique(Xt[y])
        _check_numpy_unicode_bug(classes)
        if len(np.intersect1d(classes, self.classes_)) < len(classes):
            diff = np.setdiff1d(classes, self.classes_)
            raise ValueError("y contains new labels: %s" % str(diff))
        Xt["{}__string_index".format(y)] = np.searchsorted(self.classes_, Xt[y])
        return Xt

    def get_mleap_model(self):
        js = {
          "op": self.op,
          "attributes": [{
            "name": "labels",
            "type": {
              "type": "list",
              "base": "string"
            },
            "value": self.classes_.tolist()
          }]
        }
        return js

    def get_mleap_node(self):
        js = {
          "name": self.name,
          "shape": {
            "inputs": [{
              "name": self.input_feature,
              "port": "input"
            }],
            "outputs": [{
              "name": self.output_feature,
              "port": "output"
            }]
          }
        }
        return js


class OneHotEncoder(OneHotEncoder):
    def __init__(self, input_feature, output_feature, n_values="auto", categorical_features="all",
                 dtype=np.float, sparse=True, handle_unknown='error'):
        self.op = 'one_hot_encoder'
        self.name = uid(self.op )
        self.input_feature = input_feature
        self._output_feature = output_feature
        self.output_feature = self._set_output_feature()
        self.categorical_features = "all" # Needed only for original implementation of the OneHotEncoder
        self.n_values = n_values
        self.dtype = dtype
        self.sparse = sparse
        self.handle_unknown = handle_unknown
        self.classes_=None

    def _set_output_feature(self):
        if pd.isnull(self._output_feature):
            return self.input_feature
        return self._output_feature

    def fit(self, df, y=None):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        self.classes_ = np.unique(df[self.input_feature])
        return self

    def fit_transform(self, df, y=None, **fit_params):
        """Fit OneHotEncoder to X, then transform X.

        Equivalent to self.fit(X).transform(X), but more convenient and more
        efficient. See fit for the parameters, transform for the return value.
        """
        print "here - OneHotEncoder"

        self.fit(df)

        self.classes_ = ["{}_{}".format(self.output_feature, x) for x in self.classes_]

        X = _transform_selected(df[[self.input_feature]], self._fit_transform,
                                   self.categorical_features, copy=True).toarray()

        df[self.output_feature] = X.tolist()

        return df

    def transform(self, df, y=None):
        """
        :type df: pd.DataFrame
        :param df:
        :param y:
        :return:
        """
        if y is None:
            y = self.input_feature
        X = _transform_selected(df[[y]], self._transform,
                                   self.categorical_features, copy=True).toarray()

        df["{}_one_hot".format(y)] = X.tolist()

        return df

    def get_mleap_model(self):
        js = {
            'op': self.op,
            'attributes': [{
                    'name': "size",
                    'type': "long",
                    'value': self.n_values
                }]
        }
        return js

    def get_mleap_node(self):
        js = {
          "name": self.name,
          "shape": {
            "inputs": [{
              "name": self.input_feature,
              "port": "input"
            }],
            "outputs": [{
              "name": self.output_feature,
              "port": "output"
            }]
          }
        }
        return js


class VectorAssembler(BaseEstimator, TransformerMixin):
    def __init__(self, input_columns, output_column):
        self.op='vector_assembler'
        self.name = uid(self.op)
        self.input_columns = input_columns
        self.output_column = output_column
        self.selected_features = []
        self.selected_features_unpacked = []
        self.output_unpacked = True

    def _vector_assembler(self, df):
        vector = list()
        for col in self.selected_features:
            if isinstance(df[col], float):
                vector.append(df[col])
            elif isinstance(df[col], list):
                for i in df[col]:
                    vector.append(i)
        return vector

    def _unpack_columns(self, df):
        res = {}
        for col in self.selected_features:
            if isinstance(df[col], list) and 'one_hot' in col:
                x = 0
                for i in df[col]:
                    res["{}_{}".format(col, x)] = df[col][x]
                    x+=1
        return pd.Series(res)

    def fit(self, df, y=None, **fit_params):
        for col in self.input_columns:
            # check the actual data type instead of doing df.dtypes
            if isinstance(df[col][0], float):
                self.selected_features.append(col)
                self.selected_features_unpacked.append(col)

            elif isinstance(df[col][0], list):
                classes_ = range(0, len(df[col][0]))
                column_index = ["{}_{}".format(col, x) for x in classes_]
                self.selected_features.append(col)
                self.selected_features_unpacked = self.selected_features_unpacked + column_index

        return self

    def fit_transform(self, df, y=None, **fit_params):

        # First get all of the features
        for col in self.input_columns:
            print "{} {}".format(col ,type(df[col][0]))
            # check the actual data type instead of doing df.dtypes
            if isinstance(df[col][0], float):
                self.selected_features.append(col)
                self.selected_features_unpacked.append(col)

            elif isinstance(df[col][0], list):
                classes_ = range(0, len(df[col][0]))
                column_index = ["{}_{}".format(col, x) for x in classes_]
                self.selected_features.append(col)
                self.selected_features_unpacked = self.selected_features_unpacked + column_index

        df[self.output_column] = df.apply(self._vector_assembler, axis=1)
        print "here"
        if self.output_unpacked:
            df = df.join(df.apply(self._unpack_columns, axis=1))
        print "here"
        return df

    def transform(self, df, y=None):
        # TODO: check that fit has been run

        df[self.output_column] = df.apply(self._vector_assembler, axis=1)
        if self.output_unpacked:
            df = df.join(df.apply(self._unpack_columns, axis=1))
        return df

    def get_mleap_model(self):
        js = {
            'op': self.op
        }
        return js

    def get_mleap_node(self):

        js = {
          "name": self.name,
          "shape": {
            "inputs": [{'name': x, 'port': 'input{}'.format(self.selected_features_unpacked.index(x))} for x in self.selected_features_unpacked],
            "outputs": [{
              "name": self.output_column,
              "port": "output"
            }]
          }
        }
        return js


class StandardScaler(StandardScaler):
    def __init__(self, input_features, output_feature, copy=True, with_mean=True, with_std=True):
        self.op = 'standard_scaler'
        self.name = uid(self.op )
        self.input_features = input_features
        self._output_feature = output_feature
        self.output_feature = self._set_output_feature()
        self.with_mean = with_mean
        self.with_std = with_std
        self.copy = copy

    def _set_output_feature(self):
        if pd.isnull(self._output_feature):
            return self.input_feature
        return self._output_feature

    def fit(self, df):

        Xt = df[self.input_features]

        return super(StandardScaler, self).fit(Xt)

    def fit_transform(self, df, y=None, copy=None):

        Xt = df[self.input_features]

        fit = super(StandardScaler, self).fit(Xt)

        Xt = super(StandardScaler, self).transform(Xt)

        df[self.output_feature] = Xt.tolist()

        df = df.join(pd.DataFrame(Xt, columns=["{}_scaled".format(x) for x in self.input_features]))

        return df

    def transform(self, df, y=None, copy=None):

        Xt = df[self.input_features]

        Xt = super(StandardScaler, self).transform(Xt)

        df[self.output_feature] = Xt.tolist()

        return df

    def get_mleap_model(self):

        attributes = []
        if self.with_mean is True:
            attributes.append({
                'name': 'mean',
                'type': {
                    'type': 'tensor',
                    'tensor': {
                        'base': 'double',
                        'dimensions': [-1]
                    }
                },
                'value': self.mean_
            })

        if self.with_std is True:
            attributes.append({
                'name': 'std',
                'type': {
                    'type': 'tensor',
                    'tensor': {
                        'base': 'double',
                        'dimensions': [-1]
                    }
                },
                'value': [np.sqrt(x) for x in self.var_]
            })


        js = {
          "op": self.op,
          "attributes": attributes
        }
        return js

    def get_mleap_node(self):

        js = {
          "name": self.name,
          "shape": {
            "inputs": [{
              "name": self.input_features,
              "port": "input"
            }],
            "outputs": [{
              "name": self.output_feature,
              "port": "output"
            }]
          }
        }
        return js

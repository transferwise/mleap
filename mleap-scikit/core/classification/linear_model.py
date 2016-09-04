from sklearn.linear_model import LogisticRegression
from mleap.core.utils.transformer import uid
import pandas as pd


class LogisticRegression(LogisticRegression):
    def __init__(self, vector_assebler, input_features, dependent_variable, penalty='l2', dual=False, tol=1e-4, C=1.0,
                 fit_intercept=True, intercept_scaling=1, class_weight=None,
                 random_state=None, solver='liblinear', max_iter=100,
                 multi_class='ovr', verbose=0, warm_start=False, n_jobs=1):
        self.vector_assembler = vector_assebler
        self.op='logistic_regression'
        self.name = uid(self.op)
        self.dependent_variable = dependent_variable
        self.input_features = input_features
        self.penalty = penalty
        self.dual = dual
        self.tol = tol
        self.C = C
        self.fit_intercept = fit_intercept
        self.intercept_scaling = intercept_scaling
        self.class_weight = class_weight
        self.random_state = random_state
        self.solver = solver
        self.max_iter = max_iter
        self.multi_class = multi_class
        self.verbose = verbose
        self.warm_start = warm_start
        self.n_jobs = n_jobs

    def fit(self, df, y=None, sample_weight=None):
        X = pd.DataFrame(df[self.input_features].values.tolist(), columns=self.vector_assembler.selected_features_unpacked)
        y = df[self.dependent_variable]
        super(LogisticRegression, self).fit(X, y)

        return df

    def fit_transform(self, df, y=None, prediction_column='y_hat', sample_weight=None):
        y = df[self.dependent_variable]

        X = pd.DataFrame(df[self.input_features].values.tolist(), columns=self.vector_assembler.selected_features_unpacked)
        super(LogisticRegression, self).fit(X, y)
        df[prediction_column] = super(LogisticRegression, self).predict_proba(X).tolist()
        return df

    def transform(self, df, y=None, prediction_column='y_hat'):
        #TODO: check that fit has been run
        X = pd.DataFrame(df[self.input_features].values.tolist(), columns=self.vector_assembler.selected_features_unpacked)
        df[prediction_column] = super(LogisticRegression, self).predict_proba(X).tolist()
        return df

    def predict_proba(self, df):
        X = pd.DataFrame(df[self.input_features].values.tolist(), columns=self.vector_assembler.selected_features_unpacked)
        return super(LogisticRegression, self).predict_proba(X)

    def predict_log_proba(self, df):
        X = pd.DataFrame(df[self.input_features].values.tolist(), columns=self.vector_assembler.selected_features_unpacked)
        return super(LogisticRegression, self).predict_log_proba(X)

    def get_mleap_model(self):
        js = {
            'op': self.op,
            "attributes": [{
            "name": "coefficients",
            "type": {
              "type": "tensor",
              "tensor": {
                "base": "double",
                "dimensions": [-1]
              }
            },
            "value": self.coef_.tolist()
          }, {
            "name": "intercept",
            "type": "double",
            "value": self.intercept_.tolist()
          }, {
            "name": "num_classes",
            "type": "long",
            "value": 2
          }]
        }
        return js

    def get_mleap_node(self):

        js = {
          "name": self.name,
          "shape": {
            "inputs": [{
              "name": self.input_features,
              "port": "features"
            }],
            "outputs": [{
              "name": "label_prediction",
              "port": "prediction"
            }, {
              "name": "label_probability",
              "port": "probability"
            }]
          }
        }
        return js
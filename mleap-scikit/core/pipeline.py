from sklearn.pipeline import Pipeline
import six
from mleap.core.utils.transformer import uid


__CATEGORICAL_TRANSFORMERS__ = ['LabelEncoder', 'OneHotEncoder']


class MleapPipeline(Pipeline):
    def __init__(self, steps):
        self.op = 'pipeline'
        self.name = uid(self.op)
        self.steps = steps
        self.pipeline = []

    def _pre_transform(self, X, y=None, **fit_params):
        """
        :type X: pd.DataFrame
        :param X:
        :param y:
        :param fit_params:
        :return:
        """
        fit_params_steps = dict((step, {}) for step, _ in self.steps)
        for pname, pval in six.iteritems(fit_params):
            step, param = pname.split('__', 1)
            fit_params_steps[step][param] = pval
        Xt = X

        for name, transform in self.steps[:-1]:
            if hasattr(transform, "fit_transform"):
                Xt = transform.fit_transform(Xt, y, **fit_params_steps[name])
            else:
                Xt = transform.fit(Xt, y, **fit_params_steps[name]) \
                              .transform(Xt)
        return Xt, fit_params_steps[self.steps[-1][0]]

    def get_mleap_model(self):
        js = {
            'op': self.op,
            'attributes': [{
                    'name': "nodes",
                    'type': {
                        'type': 'list',
                        'base': 'string'
                    },
                    'value': [x[0] for x in self.steps]
                }]
        }
        return js

    def get_mleap_node(self):
        js = {
            'name': self.name,
            'shape': {
                'inputs': [],
                'outputs': []
            }
        }
        return js
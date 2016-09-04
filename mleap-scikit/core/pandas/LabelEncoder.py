from . import *


class LabelEncoder(LabelEncoder):
    def __init__(self, categorical_variables):
        self.categorical_variables = categorical_variables
        self.string_index = {}

    def fit(self, df):
        """

        :param df:
        :type df: pd.DataFrame
        :return: self
        """
        for var in self.categorical_variables:
            le = LabelEncoder()
            le.fit(df[var])
            classes_, y = np.unique(le.classes_, return_inverse=True)
            self.string_index[var] ={
                'labels': classes_,
                'indicies': y
            }
        return self

    def transform(self, df):
        """

        :param df:
        :type df: pd.DataFrame
        :return:
        """
        #TODO: check that all categorical columns exist

        # Get intersecting columns
        for var in self.categorical_variables:
            #TODO: check that no new classes exist

            mapping = dict(zip(self.string_index[var]['labels'], self.string_index[var]['indicies']))
            df.replace({var: mapping}, inplace=True)

        return df

    def fit_transform(self, df):
        """
        :type df: pd.DataFrame
        :param df:
        :return:
        """
        # Fit the label encoders for all categorical variables
        self.fit(df)

        return self.transform(df)



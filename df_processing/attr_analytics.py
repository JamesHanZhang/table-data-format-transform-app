import pandas as pd

class AttrAnalytics:
    def __init__(self):
        pass

    @classmethod
    def create_attr_on_context(cls, func, df: pd.DataFrame, *args, **kwargs) -> list:
        """
        :param func: function must contain at least 2 arguments in order: DataFrame, position of index
        usage: this func can also be applied to work on calculation based on both different rows and columns simultaneously
        """
        df = df.reset_index(drop=True)
        row_num = df.index.size
        new_col = list()
        for pos in range(row_num):
            new_col.append(func(df, pos, *args, **kwargs))
        return new_col

    @classmethod
    def create_attr_by_series(cls, func, series: pd.Series) -> pd.Series:
        """
        :param func: function must and only contain one argument: element in chosen series
        """
        new_series = series.apply(func)
        return new_series

    @classmethod
    def create_attr_by_df(cls, func, df: pd.DataFrame) -> pd.Series:
        """
        :param func: function must and only contain one argument: series in pd.Series type and return one element in series
        """
        new_series = df.apply(func, axis=1)
        return new_series


if __name__ == "__main__":
    df = pd.DataFrame({'A': [1, 2], 'B': [10, 20]})
    print(df)
    def new_col(df, pos, num):
        res = df['A'][pos]+df['B'][pos]+num
        return res

    df['C'] = AttrAnalytics.create_attr_on_context(new_col, df, 100)
    print(df)
    """
        A   B    C
    0  1  10  111
    1  2  20  122
    """

    def new_series(series: pd.Series) -> str:
        element = str(series.A) + " as output"
        return element

    df['D'] = AttrAnalytics.create_attr_by_df(new_series, df)
    print(df)
    """
       A   B    C            D
    0  1  10  111  1 as output
    1  2  20  122  2 as output
    """

    def new_element(element):
        element = element + 1000
        return element

    df['E'] = AttrAnalytics.create_attr_by_series(new_element, df.A)
    print(df)
    """
       A   B    C            D     E
    0  1  10  111  1 as output  1001
    1  2  20  122  2 as output  1002
    """

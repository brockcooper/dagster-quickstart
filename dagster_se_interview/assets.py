from dagster import asset
from pandas import DataFrame, read_html, get_dummies, to_numeric
from sklearn.linear_model import LinearRegression as Regression

@asset
def country_stats() -> DataFrame:
    """ Raw country population data """
    df = read_html("https://tinyurl.com/mry64ebh", flavor='html5lib')[0]
    df.columns = ["country", "continent", "region", "pop_2022", "pop_2023", "pop_change"]
    df["pop_change"] = ((to_numeric(df["pop_2023"]) / to_numeric(df["pop_2022"])) - 1)*100
    return df

@asset
def change_model(country_stats: DataFrame) -> Regression:
    """ A regression model for each continent """
    data = country_stats.dropna(subset=["pop_change"])
    dummies = get_dummies(data[["continent"]])
    return Regression().fit(dummies, data["pop_change"])

@asset
def continent_stats(country_stats: DataFrame, change_model: Regression) -> DataFrame:
    """ Summary of continent populations and change """
    # TODO 
    return 
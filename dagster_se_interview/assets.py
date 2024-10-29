from dagster import asset
from pandas import DataFrame, read_html, get_dummies, to_numeric
from sklearn.linear_model import LinearRegression as Regression

@asset
def country_stats() -> DataFrame:
    """ Raw country population data """
    df = read_html("https://tinyurl.com/mry64ebh", flavor='html5lib')[0]
    print(df.head())
    df.columns = ["country", "pop_2022", "pop_2023", "pop_change", "continent", "region"]
    # df.columns = ["country", "continent", "region", "pop_2022", "pop_2023", "pop_change"]
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
    """ Summary of continent populations and predicted change """
    # Calculate basic stats
    continent_summary = country_stats.groupby("continent").agg(
        avg_pop_2022=("pop_2022", "mean"),
        avg_pop_2023=("pop_2023", "mean"),
        avg_pop_change=("pop_change", "mean"),
        total_pop_2022=("pop_2022", "sum"),
        total_pop_2023=("pop_2023", "sum")
    ).reset_index()
    
    # Prepare dummy variables for prediction
    dummies = get_dummies(continent_summary[["continent"]])
    
    # Predict population change using the model
    continent_summary["predicted_pop_change"] = change_model.predict(dummies)
    
    # Calculate the expected 2024 population based on predicted change
    continent_summary["predicted_pop_2024"] = (
        continent_summary["avg_pop_2023"] * (1 + continent_summary["predicted_pop_change"] / 100)
    )
    
    return continent_summary

if __name__ == "__main__":
    result = country_stats()
    print(result)
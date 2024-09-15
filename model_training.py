import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
import numpy as np
import pickle
import warnings

warnings.filterwarnings('ignore')

# MongoDB Connection
CONNECTION_STRING = "mongodb+srv://2020s17981:pKEesWvsHOvMU3gl@leptocluster.qzu48.mongodb.net/?retryWrites=true&w=majority&appName=leptocluster"
client = MongoClient(CONNECTION_STRING)
db = client['Leptospirosis_Data']
collection = db['sri_lanka']

# Load Data
def load_data():
    documents = list(collection.find())
    df = pd.DataFrame(documents)
    df = df.drop('_id', axis=1)
    return df

# Preprocess Data
def preprocess_data(df):
    y = df['Cases']
    X = df.drop('Cases', axis=1)

    # Handling categorical features
    encoder = ColumnTransformer(
        transformers=[
            ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False), ['Region'])
        ],
        remainder='passthrough'
    )

    X = encoder.fit_transform(X)
    feature_names = encoder.get_feature_names_out()
    selected_features_name = feature_names[:-1]
    feature_names_adjusted = [name.split('_', 3)[-1] for name in selected_features_name]
    feature_names_adjusted.append('Year')

    X = pd.DataFrame(X, columns=feature_names_adjusted)

    return X, y, encoder

# Define a pipeline for machine learning models
def create_ml_pipeline(model):
    return Pipeline(steps=[
        ('preprocessor', ColumnTransformer(
            transformers=[
                ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False), ['Region'])
            ],
            remainder='passthrough'
        )),
        ('model', model)
    ])

# Train Machine Learning Models
def train_ml_models(X_train, y_train):
    models = {
        'LinearRegression': LinearRegression(),
        'RandomForestRegressor': RandomForestRegressor(random_state=42),
        'DecisionTreeRegressor': DecisionTreeRegressor(random_state=42),
        'XGBRegressor': XGBRegressor(random_state=42)
    }

    param_grids = {
        'RandomForestRegressor': {
            'model__n_estimators': [100, 200, 300],
            'model__max_depth': [10, 20, 30],
            'model__min_samples_split': [2, 5, 10]
        },
        'XGBRegressor': {
            'model__n_estimators': [100, 200, 300],
            'model__max_depth': [10, 20, 30],
            'model__learning_rate': [0.01, 0.05, 0.1]
        }
    }

    best_models = {}
    for name, model in models.items():
        if name in param_grids:
            pipeline = create_ml_pipeline(model)
            grid_search = GridSearchCV(estimator=pipeline, param_grid=param_grids[name], cv=5)
            grid_search.fit(X_train, y_train)
            best_models[name] = grid_search.best_estimator_
        else:
            pipeline = create_ml_pipeline(model)
            pipeline.fit(X_train, y_train)
            best_models[name] = pipeline

    return best_models

# Train Time Series Models
def train_time_series_models(df):
    time_series_models = {}

    # ARIMA Model
    arima_model = ARIMA(df['Cases'], order=(5, 1, 0))
    arima_model_fit = arima_model.fit()
    time_series_models['ARIMA'] = arima_model_fit

    # Prophet Model
    prophet_df = df[['Year', 'Cases']].rename(columns={'Year': 'ds', 'Cases': 'y'})
    prophet_model = Prophet()
    prophet_model.fit(prophet_df)
    time_series_models['Prophet'] = prophet_model

    return time_series_models

# Evaluate Models
def evaluate_models(models, X_train, X_test, y_train, y_test):
    metrics = {}

    for name, model in models.items():
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        mae = mean_absolute_error(y_test, y_test_pred)
        mse = mean_squared_error(y_test, y_test_pred)
        rmse = mean_squared_error(y_test, y_test_pred, squared=False)
        r2 = r2_score(y_test, y_test_pred)

        metrics[name] = {'MAE': mae, 'MSE': mse, 'RMSE': rmse, 'R2': r2}

    return metrics

# Evaluate Time Series Models
def evaluate_time_series_models(models, df):
    metrics = {}

    # Test data is the last 20% of the data
    train_size = int(len(df) * 0.8)
    train, test = df.iloc[:train_size], df.iloc[train_size:]

    for name, model in models.items():
        if name == 'ARIMA':
            predictions = model.forecast(steps=len(test))
        elif name == 'Prophet':
            future = model.make_future_dataframe(periods=len(test), freq='Y')
            forecast = model.predict(future)
            predictions = forecast['yhat'].iloc[-len(test):]

        mae = mean_absolute_error(test['Cases'], predictions)
        mse = mean_squared_error(test['Cases'], predictions)
        rmse = np.sqrt(mse)

        metrics[name] = {'MAE': mae, 'MSE': mse, 'RMSE': rmse}

    return metrics

# Save the Best Model
def save_best_model(best_model):
    # Define the path to save the pickle file
    path = r'\\wsl.localhost\Ubuntu\home\naethree\Users\naethree\airflow\model\Sri_Lanka_Best_Model.pkl'

    # Check if the file already exists and delete it
    if os.path.exists(path):
        os.remove(path)
        print(f"Existing file '{path}' deleted.")

    # Save the best model
    with open(path, 'wb') as file:
        pickle.dump(best_model, file)
    print(f"Best model saved as '{path}'")

def main():
    df = load_data()
    X, y, encoder = preprocess_data(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    ml_models = train_ml_models(X_train, y_train)
    ml_metrics = evaluate_models(ml_models, X_train, X_test, y_train, y_test)

    ts_models = train_time_series_models(df)
    ts_metrics = evaluate_time_series_models(ts_models, df)

    all_metrics = {**ml_metrics, **ts_metrics}
    best_model_name = min(all_metrics, key=lambda x: all_metrics[x]['RMSE'])
    best_model = ml_models.get(best_model_name, ts_models.get(best_model_name))

    print(f"Best Model: {best_model_name} with RMSE: {all_metrics[best_model_name]['RMSE']}")
    save_best_model(best_model)

if __name__ == "__main__":
    main()

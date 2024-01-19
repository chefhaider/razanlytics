import mlflow
import pandas as pd

from darts.dataprocessing.transformers import Scaler
from darts import TimeSeries
from darts.models import TransformerModel
from darts.metrics import mape, smape
from sklearn.model_selection import train_test_split

import datetime
import os



# Define a function to preprocess the input data
def preprocess_data(data_path = 'data/btc-sent-f4.csv'):
    """Preprocess the input data for time series modeling."""
    # Load the CSV file containing Bitcoin sentiment data
    mlflow.log_param("data_path", data_path)  # Log the data path

    df = pd.read_csv(data_path)

    # Convert the 'time' column to datetime and set it as the index
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time')

    # Sort the DataFrame by index
    df = df.sort_index()

    # Convert the DataFrame to a float type
    df = df.astype('float')

    # Create time series for each sentiment attribute
    series_neg = TimeSeries.from_dataframe(df, 'time', 'weighted_comp')
    series_open = TimeSeries.from_dataframe(df, 'time', 'open')
    series_compound = TimeSeries.from_dataframe(df, 'time', 'compound')
    series_weighted_comp = TimeSeries.from_dataframe(df, 'time', 'weighted_comp')
    series_weighted_neg = TimeSeries.from_dataframe(df, 'time', 'weighted_neg')

    # Use Scaler to normalize each time series
    neg_scaled = Scaler()
    open_scaled = Scaler()
    compound_scaled = Scaler()
    weighted_comp_scaled = Scaler()
    weighted_neg_scaled = Scaler()

    series_neg_scaled = neg_scaled.fit_transform(series_neg)
    series_open_scaled = open_scaled.fit_transform(series_open)
    series_compound_scaled = compound_scaled.fit_transform(series_compound)
    series_weighted_comp_scaled = weighted_comp_scaled.fit_transform(series_weighted_comp)
    series_weighted_neg_scaled = weighted_neg_scaled.fit_transform(series_weighted_neg)
    series_scaled = [series_neg_scaled, series_open_scaled, series_compound_scaled, series_weighted_comp_scaled, series_weighted_neg_scaled]

    train_data, test_data = zip(*[train_test_split(series, test_size=0.2, random_state=None) for series in series_scaled])
    return train_data, test_data



# Define a function to train the time series model
def train_model(train_data, input_chunk_length = 24, output_chunk_length = 12, n_epochs = 46, random_state = 0, model_path = 'articats/model/model5.pth.tar'):
    """Train the multivariate time series model for Bitcoin price prediction."""
    
    # replicating previous copy of the model
    previous_model_filename = os.path.basename(model_path)
    current_date = datetime.datetime.today().strftime('%Y%m%d')
    new_filename = f'{current_date}_{previous_model_filename}'
    os.rename(model_path, os.path.join(os.path.dirname(model_path), new_filename))

    
    # Load a pre-trained TransformerModel

    mlflow.log_param("input_chunk_length", input_chunk_length)
    mlflow.log_param("output_chunk_length", output_chunk_length)
    mlflow.log_param("n_epochs", n_epochs)
    mlflow.log_param("random_state", random_state)

    mx = TransformerModel.load_model(model_path)

    # Create a new TransformerModel and fit it to the training data
    model = TransformerModel(input_chunk_length=input_chunk_length, output_chunk_length=output_chunk_length, n_epochs=n_epochs, random_state=random_state)
    model.fit(train_data, verbose=True)

    # Save the trained model
    model.save_model(model_path)

    return model

# Define a function to etestuate the trained model
def evaluate_model(test_data, model_path = 'articats/model/model5.pth.tar'):
    """Evaluate the performance of the trained model."""

    model = TransformerModel.load(model_path)
    open, open_test = test_data[:-32], test_data[-32:]
    pred = model.predict(n=12, series=open)

    mape_testue = mape(pred,open_test)
    mlflow.log_metric("MAPE", mape_testue)

    return model

# Compose the MLflow pipeline
pipeline = mlflow.pipelines.Pipeline(
    steps=[
        mlflow.pipelines.step(preprocess_data, "preprocessed_data", output=["train_data", "test_data"]),
        mlflow.pipelines.step(train_model, "trained_model", inputs={"train_data": "preprocessed_data.train_data"}),
        mlflow.pipelines.step(evaluate_model, "model_evaluation", inputs={"test_data": "preprocessed_data.test_data"}),
    ]
)



# Execute the pipeline within an MLflow run
with mlflow.start_run():


    # Run the MLflow pipeline
    pipeline.run()
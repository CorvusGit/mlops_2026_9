"""
Application
"""

import os

from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel
import pandas as pd

from src.inference import load_model, predict

import mlflow.pyfunc
import pandas as pd

def load_model(model_uri: str):
    # model_uri может быть путем к папке или "models:/name/version"
    return mlflow.pyfunc.load_model(model_uri)

def predict(model, data: pd.DataFrame):
    # MLFlow pyfunc принимает pandas DataFrame и возвращает numpy array/pandas series
    return model.predict(data)

# Load model
logger.info("Loading model")
MODEL_PATH = os.path.join("models", "model.joblib")
MODEL = load_model(MODEL_PATH)
logger.info("Model loaded")

app = FastAPI()

class IrisFeatures(BaseModel):
    """Features for Iris dataset"""
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float

@app.get("/")
def health_check() -> dict:
    """Health check"""
    return {"status": "ok"}

@app.post("/predict")
def make_prediction(features: IrisFeatures) -> dict:
    """Make a prediction by model"""
    try:
        data = pd.DataFrame([features.model_dump()])
        prediction = predict(MODEL, data)
        classes = ["setosa", "versicolor", "virginica"]
        pred_class = classes[prediction[0]]
        logger.info(f"Prediction: {pred_class}")
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail="Prediction error")
    
    return {"prediction": pred_class}
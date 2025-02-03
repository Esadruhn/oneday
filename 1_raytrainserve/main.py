from starlette.requests import Request
from typing import Dict, Tuple, Any
from configparser import ConfigParser

import ray
from ray.data import Dataset, Preprocessor
from ray.data.preprocessors import StandardScaler
from ray.train.xgboost import XGBoostTrainer
from ray.train import Result, ScalingConfig, Checkpoint
from ray import serve

import xgboost
import pathlib
import pandas as pd


def _parse_config() -> ConfigParser:
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / "config.ini")
    return config


def _get_config_path(config_value: str) -> pathlib.Path:
    return pathlib.Path(__file__).parent / config_value


def _prepare_data(train_config) -> Tuple[Dataset, Dataset, Dataset]:
    dataset_path = pathlib.Path(train_config["dataset"]).resolve()
    dataset = ray.data.read_csv(dataset_path)
    train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)
    test_dataset = valid_dataset.drop_columns(["target"])
    return train_dataset, valid_dataset, test_dataset


def train_xgboost(
    train_dataset: Dataset,
    valid_dataset: Dataset,
    checkpoint_path: pathlib.Path,
    num_workers: int,
    use_gpu: bool = False,
) -> Result:

    # Scale some random columns
    columns_to_scale = ["mean radius", "mean texture"]
    preprocessor = StandardScaler(columns=columns_to_scale)
    train_dataset = preprocessor.fit_transform(train_dataset)
    valid_dataset = preprocessor.transform(valid_dataset)

    # XGBoost specific params
    params = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }

    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        label_column="target",
        params=params,
        datasets={"train": train_dataset, "valid": valid_dataset},
        num_boost_round=100,
        metadata={"preprocessor_pkl": preprocessor.serialize()},
    )
    result: Result = trainer.fit()
    print(result.metrics)

    result.get_best_checkpoint(metric="valid-logloss", mode="min").to_directory(
        checkpoint_path
    )

    return result


class Predict:

    def __init__(self, checkpoint: Checkpoint):
        self.model = XGBoostTrainer.get_model(checkpoint)
        self.preprocessor = Preprocessor.deserialize(
            checkpoint.get_metadata()["preprocessor_pkl"]
        )

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        preprocessed_batch = self.preprocessor.transform_batch(batch)
        dmatrix = xgboost.DMatrix(preprocessed_batch)
        return {"predictions": self.model.predict(dmatrix)}


def predict_xgboost(checkpoint: Checkpoint, test_dataset: Dataset):

    scores = test_dataset.map_batches(
        Predict,
        fn_constructor_args=[checkpoint],
        concurrency=1,
        batch_format="pandas",
    )

    predicted_labels = scores.map_batches(
        lambda df: (df > 0.5).astype(int), batch_format="pandas"
    )
    return predicted_labels.to_pandas().to_dict(orient="list")


@serve.deployment(name="xgboost")
class XGBoostDeployment:
    def __init__(self, checkpoint_path):
        # Initialize model state:
        self._model = Checkpoint(path=checkpoint_path)

    async def __call__(self, http_request: Request):
        data: list[dict[str, Any]] = await http_request.json()
        predicted_labels = predict_xgboost(self._model, ray.data.from_items(data))
        return predicted_labels


def initialize():
    config = _parse_config()
    checkpoint_path = _get_config_path(config["FILES"]["checkpoint"])
    if config["TRAIN"].getboolean("train"):
        train_dataset, valid_dataset, test_dataset = _prepare_data(config["TRAIN"])
        test_dataset.write_csv(_get_config_path(config["FILES"]["test_dataset"]))
        train_xgboost(
            train_dataset,
            valid_dataset,
            checkpoint_path=_get_config_path(config["FILES"]["checkpoint"]),
            num_workers=config["TRAIN"].getint("num_workers"),
            use_gpu=config["TRAIN"].getboolean("use_gpu"),
        )
    elif not checkpoint_path.exists():
        raise Exception(
            "No checkpoint model found, please set 'train=True' in the config file then run the script again."
        )
    else:
        return XGBoostDeployment.bind(checkpoint_path)


initialize()

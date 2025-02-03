# Ray train serve

This is a one-day project, the goal is to use Ray to train and then serve an XGBoost model. I chose an XGBoost model example
because it is light-weight, fast to re-train and serve.

What happens at execution time:

- Train an XGBoost model on the [Breast Cancer example dataset](), see it in `data/breast_cancer.csv`
- Save the model in `data/checkpoint`
- Load the checkpoint then serve it with a Ray server
- Then you can query the model with data from the test split of the breast cancer dataset

## How to

You can update the ray config in the `config.ini` file.
For the first iteration you need to train the model so ensure that `train` is set to `True` in the configuration file.

```yaml
[TRAIN]
# Set to True for the first iteration
train = True
```

To change the ray serve deployment configuration, edit the `serve_config.yaml` file.

```bash
pip install -r requirements.txt

# Trains the model then saves it to data/checkpoint
# then start the ray server (load and serve the model)
serve run serve_config.yaml

# Query the server
python query.py
```

## Useful links

- [ray](https://docs.ray.io/en/latest/index.html)
- [ray tutorial: Training a model with distributed XGBoost](https://docs.ray.io/en/latest/train/examples/xgboost/xgboost_example.html)

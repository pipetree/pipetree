[![codecov](https://codecov.io/gh/pipetree/pipetree/branch/master/graph/badge.svg)](https://codecov.io/gh/pipetree/pipetree)
# Pipetree
A minimalist data pipeline library built on top of Spark

### Example: Concurrently test 4 learning rates for your Cat Emotion Predictor in 32 lines of code

* Compatible with most machine learning libraries (TensorFlow / Keras / Theano / Caffe / ... )
* Caches all intermediate computations, allowing for rapid prototyping.
* Automatically creates and manages a temporary EC2 cluster, including dependencies and server setup

## Configure your pipeline with JSON
```json
{
  "raw_images": {
    "type": "file_folder",
    "filepath":  "./local_path_to_cat_images/",
  },
  "preprocess_parameters": {
    "whiten": true,
    "PCA": false
  },
  "preprocessed_images": {
    "inputs": ["raw_images", "preprocess_parameters"],
    "outputs": "file_folder",
    "run": "my_image_preprocess_function"
  },
  "model_parameters": {
    "number_hidden_neurons": 100,
    "epochs": 200
  },
  "test_parameters": {
    "type": "grid_search_parameters",
    "learning_rate": [0.1, 0.01, 0.001, 0.2]
  },
  "trained_model": {
    "inputs": ["preprocessed_images", "model_parameters", "test_parameters"],
    "outputs": "file_folder",
    "run": "my_train_model_function"
  }
}
```

## Python integration
```python
# Function takes arguments in the same order as the pipeline item inputs
def my_image_preprocess_function(raw_images, preprocess_parameters):
  if preprocess_parameters["PCA"] is True:
    pipetree.log("PCA enabled")
  for image in raw_images:
    yield some_preprocess_function(image)

def my_train_model_function(preprocessed_images, model_parameters, test_parameters):
  hidden_neurons = int(model_parameters["number_hidden_neurons"])
  learning_rate = float(test_parameters["learning_rate"])
  epochs = int(model_parameters["epochs"])
  for i in xrange(epochs):
    (error, trained_model) = train_model_one_epoch()
    # Produce trained model objects to be stored as pipeline artifacts
    yield { "data": trained_model.serialize(),
      "metadata": { "error": float(error) }
  
```

## Future Features

* Use builtin spark datatypes
* Smart handling of file_folders (stream to s3 as they get produced, from s3 as they are used)
* Allow mapping over files for parallel preprocessing



[![codecov](https://codecov.io/gh/pipetree/pipetree/branch/master/graph/badge.svg)](https://codecov.io/gh/pipetree/pipetree)
# Pipetree
A minimalist data pipeline library built on top of Spark

### Example: Concurrently test 4 learning rates for your Cat Emotion Predictor in 32 lines of code


* Compatible with most machine learning libraries (TensorFlow / Keras / Theano / Caffe / ... )
* Caches all intermediate computations, allowing for rapid prototyping.
* Automatically creates and manages a temporary EC2 cluster, including dependencies and server setup

```python
# Images will be automatically uploaded to s3 from your local machine
raw_images = pipetree.file_folder("cat_pictures", "./local_path_to_cat_images/")

# Images will only be preprocessed once, then stored on s3
preprocess_images = {
  "name": "preprocessed_images",
  "inputs": { "images": raw_images },
  "outputs": { "images": "file_folder" },
  "run": my_image_preprocess_function
}

# Change parameters at any time and a new model will be trained.
params = pipetree.parameters({ "number_hidden_neurons": 100, "epochs": 200 })
test_params = pipetree.grid_search_parameters({ "learning_rate": [0.001, 0.01, 0.1, 0.2] })

# One model will automatically be trained for each learning rate
trained_model = {
  "name": "trained_model",
  "inputs": { "images": preprocessed_images, "params": params , "test_params": test_params },
  "outputs": { "trained_model": "file" },
  "run": my_training_function
}

# Setup your pipeline with straightforward options
pipeline_options = {
  "pipeline_name": "predict_cat_emotions"
  "storage": "s3",
  "cluster": { "max_servers": 10, "server_size": "c2" }
}

pipetree.run(trained_model, pipeline_options)
```

## Future Featurse

* Use builtin spark datatypes
* Smart handling of file_folders (stream to s3 as they get produced, from s3 as they are used)
* Allow mapping over files for parallel preprocessing



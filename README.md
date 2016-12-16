# Pipetree
A minimalist data pipeline library built on top of Spark

### Example: Concurrently test learning rates for your Cat Emotion Predictor in 32 lines of code

Pipetree is simple and straight forward. 

```python
# Images will be automatically uploaded to s3 from your local machine
raw_images = pipetree.file_folder("cat_pictures", "./local_path_to_cat_images/")

# Images will only be preprocessed once, then stored on s3
preprocess_images = {
  "name": "preprocessed_images",
  "inputs": {"images": raw_images},
  "outputs": {"images": "file_folder"},
  "run": my_image_preprocess_function
}

# Change parameters at any time and a new model will be trained.
params = pipetree.parameters({"number_hidden_neurons": 100, "epochs": 200})
test_params = pipetree.grid_search_parameters({"learning_rate": [0.001, 0.01, 0.1, 0.2]})

# One model will automatically be trained for each learning rate
trained_model = {
  "name": "trained_model",
  "inputs": {"images": preprocessed_images, "params": params , "test_params": test_params },
  "outputs": {"trained_model": "file"},
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


##Deploy a resumable, cached, machine learning pipeline with to EC2 in 10 lines of code. 

* Compatible with any machine learning library (TensorFlow / Keras / Theano / Caffe / ... )
* Runs locally or on EC2 utilizing S3 for storage.
* Caches all intermediate computations, allowing for rapid prototyping.


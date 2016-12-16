# Pipetree
A minimalist data pipeline library built on top of Spark

### Example: Test all learning rates for your Cat Emotion Predictor in 31 lines of code

Pipetree uses basic interfaces, and only has a handful functions, all of which are obvious. 

```python
raw_images = pipetree.file_folder("cat_pictures", "./local_path_to_cat_images/")

preprocess_images = {
  "name": "preprocessed_images",
  "inputs": {"images": raw_images},
  "outputs": {"images": "file_folder"},
  "run": lambda inputs: my_preprocess(inputs["images"])
}

parameters = pipetree.parameters({"number_hidden_neurons": 100})
test_parameters = pipetree.grid_search_parameters({"learning_rate": [0.001, 0.01, 0.1, 0.2]})

trained_model = {
  "name": "trained_model",
  "inputs": {"images": preprocessed_images,
  	    "parameters": parameters,
	    "test_parameters": test_parameters
  	    },
  "outputs": {"trained_model": "file"},
   "run": my_training_function
}

options = {
  "pipeline_name": "predict_cat_emotions"
  "storage": "s3",
  "cluster": {
    "max_servers": 10,
    "server_size": "c2"
  }	
}

pipetree.run(trained_model, options)
```


##Deploy a resumable, cached, machine learning pipeline with to EC2 in 10 lines of code. 

* Compatible with any machine learning library (TensorFlow / Keras / Theano / Caffe / ... )
* Runs locally or on EC2 utilizing S3 for storage.
* Caches all intermediate computations, allowing for rapid prototyping.


# Pipetree
A minimalist data pipeline library built on top of Spark


```python
raw_images = pipeTree.file_folder("cat_pictures", "./local_path_to_cat_images/")
preprocess_images = {
  "name": "preprocessed_images",
  "inputs": {"images": raw_images},
  "outputs": {"images": "file_folder"},
  "run": lambda inputs: my_preprocess(inputs["images"])
}

parameters = pipeTree.parameters({"learning_rate": 0.01})
trained_model = {
  "name": "trained_model",
  "inputs": {"images": preprocessed_images,
  	    "parameters": parameters
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

pipeTree.run(trained_model, options)
```


##Deploy a resumable, cached, machine learning pipeline with to EC2 in 10 lines of code. 

* Compatible with any machine learning library (TensorFlow / Keras / Theano / Caffe / ... )
* Runs locally or on EC2 utilizing S3 for storage.
* Caches all intermediate computations, allowing for rapid prototyping.


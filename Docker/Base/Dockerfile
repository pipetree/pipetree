FROM gcr.io/tensorflow/tensorflow:latest-gpu

RUN apt-get update; \
        apt-get install -y \
    	python python-pip \
	python-numpy python-scipy \
	build-essential python-dev python-setuptools \
	emacs24 python-skimage \
	libssl-dev git python3 wget


# We need to install the security extras for Python < 2.7.9
RUN pip install --upgrade requests[security]

# Setup virtualenv (future use)
RUN pip install virtualenv &&\
    mkdir pipetree-env &&\
    sudo virtualenv -p python3 pipetree-env

# Install python3.5 (currently employed)
RUN wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz &&\
    sudo tar xzf Python-3.5.2.tgz &&\
    cd Python-3.5.2 &&\
    sudo ./configure &&\
    sudo make altinstall

# Setup generally used pip packages for users
RUN pip3.5 install -U pip &&\
    pip3.5 install -U scikit-learn keras flask pandas
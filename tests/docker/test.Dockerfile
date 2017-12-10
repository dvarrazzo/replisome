FROM replisome/base

RUN apt-get install -y python python-setuptools python-pip libyaml-dev
RUN pip install -U pip

ADD tests/pytests/requirements.txt /code/tests/pytests/
RUN pip install -r /code/tests/pytests/requirements.txt

ADD ./ /code
WORKDIR /code

RUN python setup.py install

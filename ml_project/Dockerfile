FROM python:3.9.4

WORKDIR /ml_project/

COPY . /ml_project/

RUN pip install --upgrade pip && \
    pip install --ignore-installed -r /ml_project/requirements.txt

RUN chmod u+x ./start.sh
FROM python:2.7

WORKDIR autoscaler
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
ADD src/ .
ADD autoscaler.conf .

CMD ["/usr/local/bin/python", "autoscaler.py"]

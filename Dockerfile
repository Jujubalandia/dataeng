FROM python:3.7-slim 

WORKDIR /srv
ADD ./requirements.txt /srv/requirements.txt
RUN pip install -r requirements.txt
ADD . /srv
CMD ["python","-u","app.py"]

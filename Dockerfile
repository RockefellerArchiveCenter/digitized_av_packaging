FROM python:3.10

RUN yum update -y && yum install -y \
  ffmpeg

WORKDIR /code

ADD requirements.txt .
RUN pip install -r requirements.txt

COPY validate.py .

CMD [ "package.main" ]
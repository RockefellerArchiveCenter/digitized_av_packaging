FROM python:3.10-slim

RUN apt update -y && apt install -y \
  ffmpeg

WORKDIR /code

ADD requirements.txt .
RUN pip install -r requirements.txt

COPY package.py .

CMD [ "package.main" ]
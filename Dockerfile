FROM python:3.11-slim-buster as base
RUN apt update -y && apt install -y ffmpeg
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src src

FROM base as test
COPY test_requirements.txt .coveragerc ./
RUN pip install -r test_requirements.txt
COPY tests tests

FROM base as build
CMD [ "python", "src/package.py" ]
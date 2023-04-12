FROM python:3.10-slim as base
RUN apt update -y && apt install -y \
  ffmpeg
WORKDIR /code
ADD requirements.txt .
RUN pip install -r requirements.txt
COPY package.py .

FROM base as test
COPY test_requirements.txt .coveragerc ./
RUN pip install -r test_requirements.txt
COPY fixtures fixtures
COPY test_package.py .

FROM base as build
CMD [ "python", "package.py" ]
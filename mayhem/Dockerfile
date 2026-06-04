FROM python:3.11-slim

RUN apt-get update && apt-get install -y clang gcc build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install atheris

# Install a Cython version compatible with this fastavro source
RUN pip install "Cython<3" setuptools wheel

# Build
ADD . /src
WORKDIR /src
RUN python3 -m pip install --no-build-isolation .

CMD ["/src/mayhem/fuzz_reader.py"]

FROM pytorch/pytorch:2.6.0-cuda11.8-cudnn9-devel
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN echo 'Etc/UTC' > /etc/timezone \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && apt-get update \
    && apt-get -y --no-install-recommends install \
    nvidia-container-toolkit nvidia-utils-570 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl git wget vim unzip zip tmux \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
ENV PATH="/root/.local/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 -

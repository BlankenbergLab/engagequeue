FROM docker:27.5.1-cli@sha256:851f91d241214e7c6db86513b270d58776379aacc5eb9c4a87e5b47115e3065c AS docker-cli
FROM condaforge/miniforge3:24.11.3-0@sha256:bcfc04b3562faa8dfd48f3767e2bd7b73a8529f9611b508d42e5bd227db1342d

WORKDIR /opt/engagequeue
ENV CONDA_PKGS_DIRS=/opt/engagequeue/.cache/conda/pkgs \
    PIP_CACHE_DIR=/opt/engagequeue/.cache/pip \
    HOME=/root \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/engagequeue/.conda/engagequeue/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN /opt/conda/bin/conda create -y -p /opt/engagequeue/.conda/engagequeue -c conda-forge python=3.12 pip \
    && /opt/conda/bin/conda clean -afy
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY wheels/ /tmp/engagequeue-wheels/
RUN /opt/engagequeue/.conda/engagequeue/bin/python -m pip install --no-cache-dir --only-binary=:all: psutil==7.2.2 PyYAML==6.0.3 \
    && /opt/engagequeue/.conda/engagequeue/bin/python -m pip install --no-cache-dir --no-deps /tmp/engagequeue-wheels/*.whl \
    && /opt/engagequeue/.conda/engagequeue/bin/python -m pip check \
    && rm -rf /tmp/engagequeue-wheels /opt/engagequeue/.cache

ENV HOME=/tmp/engagequeue
WORKDIR /tmp
ENTRYPOINT ["/opt/engagequeue/.conda/engagequeue/bin/python", "-m", "engagequeue.managed_runtime"]
CMD ["keepalive"]

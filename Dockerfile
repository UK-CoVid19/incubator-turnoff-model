FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:20220418_RC00

  ARG WORKDIR=/dataflow/template
  RUN mkdir -p ${WORKDIR}
  WORKDIR ${WORKDIR}

  COPY requirements.txt .
  COPY pipeline.py .
  COPY main.py .
  COPY temp_model.py .

  # Do not include `apache-beam` in requirements.txt
  ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
  ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"


  RUN apt-get update \
    # Upgrade pip and install the requirements.
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # Download the requirements to speed up launching the Dataflow job.
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE


  ENV PIP_NO_DEPS=True
  # Install apache-beam and other dependencies to launch the pipeline
  RUN pip install apache-beam[gcp]
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /dataflow-template

COPY pipelines/cso_dataflow_pipeline.py .
COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r functions/requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=cso_dataflow_pipeline.py

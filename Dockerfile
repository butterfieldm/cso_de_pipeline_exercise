FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

WORKDIR /dataflow-template

COPY pipelines/cso_dataflow_pipeline.py .
COPY functions/requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=cso_dataflow_pipeline.py

FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/
RUN useradd --uid 10001 --no-create-home appuser
USER 10001
ENV PYTHONUNBUFFERED=1
# default command runs the detector; the producer overrides it
CMD ["python", "-m", "app.detector"]

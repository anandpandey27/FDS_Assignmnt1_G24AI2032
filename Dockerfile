FROM python:3.9
WORKDIR /app
COPY node.py .
CMD ["python", "node.py"]

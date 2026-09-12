FROM python:3.10-slim

# Set timezone (opsional tapi bagus untuk log server)
ENV TZ=Asia/Jakarta

WORKDIR /app

# Install dependencies terlebih dahulu (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin kode program
COPY consensus.py .

# Jalankan server secara terus-menerus
CMD ["python", "-u", "consensus.py"]

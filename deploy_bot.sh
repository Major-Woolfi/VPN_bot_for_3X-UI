#!/bin/bash

cd /root/bots/PhantomVPN_bot/

# ✅ ТОЛЬКО aiogram! Остальное встроенное
cat > requirements.txt << EOF
aiogram
aiohttp
aiosqlite
aiofiles
python-dotenv
paramiko
EOF

# ✅ Правильный Dockerfile
cat > Dockerfile << EOF
FROM python:3.13.9
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY data ./data
CMD ["python", "main.py"]
EOF

# Собери и запусти
sudo docker build -t phantomvpn_bot .
sudo docker rm -f phantomvpn_bot || true
sudo docker run -d \
  --name phantomvpn_bot \
  --restart always \
  -v /root/bots/PhantomVPN_bot:/app  \
  phantomvpn_bot

echo "✅ Готово!"
echo "Логи: sudo docker logs -f phantomvpn_bot"

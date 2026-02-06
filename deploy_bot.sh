#!/bin/bash

cd /root/bots/VPN_bot/

# ✅ ТОЛЬКО aiogram! Остальное встроенное
cat > requirements.txt << EOF
aiogram
aiohttp
aiosqlite
aiofiles
python-dotenv
EOF

# ✅ Правильный Dockerfile
cat > Dockerfile << EOF
FROM python:3.13.9
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
EOF

# Собери и запусти
sudo docker build -t vpn_bot .
sudo docker rm -f vpn_bot || true
sudo docker run -d \
  --name vpn_bot \
  --restart always \
  -v /root/bots/VPN_bot:/app  \
  vpn_bot

echo "✅ Готово!"
echo "Логи: sudo docker logs -f vpn_bot"
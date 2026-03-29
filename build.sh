#!/bin/bash
set -e

echo "📦 Installing Python packages..."
pip install -r requirements.txt

echo "📥 Downloading FFmpeg static binary..."
mkdir -p bin

wget -q "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz" \
  -O /tmp/ffmpeg.tar.xz

tar -xf /tmp/ffmpeg.tar.xz -C /tmp/

cp /tmp/ffmpeg-master-latest-linux64-gpl/bin/ffmpeg ./bin/ffmpeg
chmod +x ./bin/ffmpeg
rm -rf /tmp/ffmpeg.tar.xz /tmp/ffmpeg-master-latest-linux64-gpl

echo "✅ FFmpeg ready: $(./bin/ffmpeg -version | head -1)"
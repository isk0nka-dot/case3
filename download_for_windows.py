import urllib.request
import os

MODELS = {
    "yolov8n.onnx": "https://github.com/ultralytics/assets/releases/download/v8.2.0/yolov8n.onnx",
    "arcface.onnx": "https://huggingface.co/ashleykza/insightface-models/resolve/main/w600k_r50.onnx"
}

os.makedirs("Case-3/argus-backend/models", exist_ok=True)

print("Downloading models...")
for name, url in MODELS.items():
    dest = f"Case-3/argus-backend/models/{name}"
    print(f"Downloading {name}...")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req) as response, open(dest, 'wb') as out_file:
            out_file.write(response.read())
        size = os.path.getsize(dest)
        print(f"✅ {name} downloaded ({size} bytes)")
    except Exception as e:
        print(f"❌ Failed to download {name}: {e}")


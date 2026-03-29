# Streaming Service

Python-based Telegram video streaming service with HTTP range request support.

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Generate session string
python generate_session.py

# Run server
python main.py
```

## Environment Variables

Create `.env` file:
```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION=your_session_string
TELEGRAM_CHANNEL_ID=-1001234567890
PORT=3030
```

## API Endpoints

- `GET /` - Health check
- `GET /stream/<message_id>` - Stream video file (supports Range headers)
- `GET /demo` - Demo video (no Telegram required)
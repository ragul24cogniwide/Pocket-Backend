# Flick backend (FastAPI + WebSockets)
#
#   docker build -t flick-backend .
#   docker run --env-file .env -p 8000:8000 flick-backend
#
# Secrets (.env, the Firebase key) are never copied into the image (see .dockerignore): pass them
# as environment variables, e.g. FIREBASE_CREDENTIALS_JSON, or mount the key file read-only.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8000

WORKDIR /app

# Dependencies first, so code changes don't reinstall them (every package ships a ready-made
# wheel, so no compiler is needed)
COPY requirements.txt .
RUN pip install -r requirements.txt

# The code belongs to root and is read-only for the app; only the media cache is writable
RUN useradd --create-home --uid 10001 flick
COPY . .
RUN mkdir -p uploads/images uploads/audio && chown -R flick:flick uploads
USER flick

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/api/health' % os.environ.get('PORT', '8000'), timeout=4)" || exit 1

# One process on purpose: live sockets are tracked in memory (Redis links several servers).
# PORT is honoured because hosts such as Render set it.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]

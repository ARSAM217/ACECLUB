#!/data/data/com.termux/files/usr/bin/bash
set -e
cd "$(dirname "$0")"
echo "== Dor Dor setup =="
command -v node >/dev/null 2>&1 || { echo "Node.js not found. Run: pkg install nodejs"; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "npm not found. Run: pkg install nodejs"; exit 1; }
# Some Android file managers skip hidden files such as .env.example.
# Create a safe default .env if neither environment file exists.
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    cp .env.example .env
  else
    umask 077
    cat > .env <<'ENVEOF'
PORT=1074
NODE_ENV=development
SESSION_SECRET=replace-with-a-unique-random-secret-at-least-32-characters
ENVEOF
    echo "Warning: .env.example was missing; created a default .env. Set a unique SESSION_SECRET before public use."
  fi
  echo "Created .env. Change SESSION_SECRET before exposing this server."
fi
if [ ! -d node_modules/sql.js ] || [ ! -d node_modules/express ]; then npm install; fi
mkdir -p data uploads
chmod 700 data
PORT="${PORT:-1074}"
echo "Starting Dor on port ${PORT}..."
exec env PORT="$PORT" npm start

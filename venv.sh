venv() {
  if [ ! -d "venv" ]; then python -m venv venv; fi
}

activate() {
  venv
  if [ -z "$VIRTUAL_ENV" ]; then . venv/bin/activate; fi
}

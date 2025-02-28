#!/bin/bash
set -a  # automatically export all variables
source /home/Feedback_bot/.venv/Scripts/activate
set +a

# Завершаем предыдущие экземпляры main.py
pkill -f "python3 feedback_bot.py" || true

cd /home/Feedback_bot/
python3 feedback_bot.py

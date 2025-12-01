Створи та активуй віртуальне середовище:
python3 -m venv .venv
source .venv/bin/activate
Встанови залежності:
pip install requests openpyxl
Запусти:
python check_redirects.py redirects.csv bad_redirects.csv --timeout 15 --workers 10
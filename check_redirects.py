import csv
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from requests.adapters import HTTPAdapter

# ----------------------------------------
# ВАЖЛИВО: вкажи тут домен свого сайту
# ----------------------------------------
BASE_URL = "https://www.ibuyfireplaces.com"

# Глобальна сесія з пулом з'єднань
SESSION = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)

# Лок і змінні для прогресу
print_lock = threading.Lock()
progress_counter = 0
total_tasks = 0


def print_progress(msg: str):
    """Безпечний для потоків друк."""
    with print_lock:
        print(msg)


def update_progress():
    """Оновлює лічильник прогресу та друкує кожні 10 елементів."""
    global progress_counter, total_tasks
    with print_lock:
        progress_counter += 1
        pct = (progress_counter / total_tasks) * 100 if total_tasks else 0
        if progress_counter % 10 == 0 or progress_counter == total_tasks:
            print(f"Перевірено {progress_counter} / {total_tasks} ({pct:.1f}%)")


def build_full_url(path: str, base: str) -> str:
    """Формує повний URL із шляху або повертає як є, якщо це вже URL."""
    path = path.strip()
    if not path:
        return path

    lower = path.lower()
    if lower.startswith("http://") or lower.startswith("https://"):
        return path

    return base.rstrip("/") + "/" + path.lstrip("/")


def read_redirects(input_path: str):
    """Зчитування CSV без заголовків."""
    redirects = []
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue

            raw_source = row[0].strip()
            raw_target = row[1].strip()
            if not raw_source or not raw_target:
                continue

            source = build_full_url(raw_source, BASE_URL)
            target = build_full_url(raw_target, BASE_URL)

            code = 301
            if len(row) >= 3:
                try:
                    code = int(row[2])
                except ValueError:
                    code = 301

            redirects.append({
                "Source": source,
                "Target": target,
                "ExpectedCode": code,
            })

    return redirects


def normalize_url(url: str) -> str:
    """Нормалізація URL для порівняння."""
    return url.strip().rstrip("/")


def _request_with_fallback(url: str, timeout: int):
    """HEAD → (за потреби) GET."""
    try:
        resp = SESSION.head(url, allow_redirects=True, timeout=timeout)
        if resp.status_code in (405, 501) and not resp.history:
            resp = SESSION.get(url, allow_redirects=True, timeout=timeout)
        return resp
    except requests.RequestException:
        return SESSION.get(url, allow_redirects=True, timeout=timeout)


def check_one_redirect(item: dict, timeout: int = 10) -> dict:
    """Основна логіка перевірки одного редіректу."""
    source = item["Source"]
    target = item["Target"]
    expected_code = item["ExpectedCode"]

    result = {
        "Source": source,
        "Target": target,
        "ExpectedCode": expected_code,
        "FirstStatus": None,
        "FinalUrl": None,
        "Ok": False,
        "Error": "",
    }

    # 🔍 1. Перевірка: Source і Target однакові
    if normalize_url(source) == normalize_url(target):
        result["Error"] = (
            "Source і Target однакові. Налаштований редірект на ту саму URL "
            "(ймовірно, спроба редіректу 404 на саму сторінку)."
        )
        print_progress(f"[ERROR same-url] {source}")
        update_progress()
        return result

    # 🔍 2. HTTP-запит із обробкою помилок
    try:
        response = _request_with_fallback(source, timeout)
    except requests.Timeout:
        result["Error"] = f"Таймаут після {timeout} секунд"
        print_progress(f"[ERROR timeout] {source}")
        update_progress()
        return result
    except requests.TooManyRedirects as e:
        history = getattr(e, "history", []) or []
        cycle_info = "Можливий цикл редіректів."

        if history:
            result["FirstStatus"] = history[0].status_code
            last_resp = history[-1]
            last_url = last_resp.headers.get("Location", last_resp.url)
            result["FinalUrl"] = last_url

            if len(history) >= 2:
                prev_url = history[-2].url
                cycle_info = f"Можливий цикл: {prev_url} → {last_url}"
            else:
                cycle_info = f"Можливий цикл, останній URL: {last_url}"

        result["Error"] = f"Перевищено ліміт редіректів (30). {cycle_info}"
        print_progress(f"[ERROR redirect loop] {source}")
        update_progress()
        return result
    except requests.ConnectionError:
        result["Error"] = "Помилка з'єднання"
        print_progress(f"[ERROR connection] {source}")
        update_progress()
        return result
    except requests.RequestException as e:
        result["Error"] = f"HTTP-помилка: {type(e).__name__}"
        print_progress(f"[ERROR {type(e).__name__}] {source}")
        update_progress()
        return result

    # 🔍 3. Успішний запит — аналіз коду та фінального URL
    first_response = response.history[0] if response.history else response
    result["FirstStatus"] = first_response.status_code
    result["FinalUrl"] = response.url

    status_ok = first_response.status_code == expected_code
    url_ok = normalize_url(response.url) == normalize_url(target)

    result["Ok"] = status_ok and url_ok

    # Лог у консоль
    if response.history:
        print_progress(f"[{first_response.status_code}] {source} → {response.url}")
    else:
        print_progress(f"[{first_response.status_code}] {source}")

    if not status_ok:
        result["Error"] += f"Очікуваний код {expected_code}, отримано {first_response.status_code}. "

    if not url_ok:
        result["Error"] += f"Очікуваний фінальний URL {target}, отримано {response.url}."

    update_progress()
    return result


def write_failed_redirects(results: list[dict], output_path: str):
    """Записує у CSV тільки ті редіректи, які не Ok."""
    failed = [r for r in results if not r["Ok"]]

    if not failed:
        print("Усі редіректи коректні. Файл з помилками не створено.")
        return

    fieldnames = [
        "Source",
        "Target",
        "ExpectedCode",
        "FirstStatus",
        "FinalUrl",
        "Ok",
        "Error",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in failed:
            writer.writerow(row)

    print(f"Збережено {len(failed)} некоректних редіректів у файл: {output_path}")


def main():
    global total_tasks

    parser = argparse.ArgumentParser(description="Перевірка редіректів")
    parser.add_argument("input", help="CSV без заголовків (Source, Target, Code)")
    parser.add_argument("output", help="CSV з помилками")
    parser.add_argument("--workers", type=int, default=40, help="Кількість потоків")
    parser.add_argument("--timeout", type=int, default=8, help="Таймаут HTTP-запитів")

    args = parser.parse_args()

    redirects = read_redirects(args.input)
    total_tasks = len(redirects)

    print(f"Знайдено {total_tasks} редіректів для перевірки.")
    print("Старт перевірки...\n")

    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(check_one_redirect, item, args.timeout)
            for item in redirects
        ]
        for future in as_completed(futures):
            results.append(future.result())

    write_failed_redirects(results, args.output)


if __name__ == "__main__":
    main()
"""
Тестовый скрипт для проверки новых методов WB API.

Запуск:
    python scripts/test_new_methods.py

Тестирует:
    1. fetch_tariffs_commission - комиссии по категориям
    2. fetch_search_report - отчёт по поисковым позициям
    3. fetch_product_search_texts - поисковые запросы по товарам
"""

import os
import sys
import json
from datetime import date, timedelta
from dotenv import load_dotenv

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.connectors.wb import WBConnector
from src.etl.transformers import WBTransformer
from src.logging_config.logger import configure_logging

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
configure_logging()


def test_tariffs_commission(connector: WBConnector) -> bool:
    """Тест метода комиссий"""
    print("\n" + "="*60)
    print("TEST 1: fetch_tariffs_commission")
    print("="*60)

    try:
        raw_data = connector.fetch_tariffs_commission(locale="ru")

        # Проверяем структуру ответа
        report = raw_data.get("report", [])
        print(f"\n✅ Получено категорий: {len(report)}")

        if report:
            # Показываем первые 3 записи
            print("\nПример данных (первые 3 записи):")
            for item in report[:3]:
                print(f"  - {item.get('subjectName')} (ID: {item.get('subjectID')})")
                print(f"    FBS: {item.get('kgvpMarketplace')}%, FBW: {item.get('paidStorageKgvp')}%")

            # Ищем БАДы и пищевые добавки
            print("\n🔍 Поиск БАДов и пищевых добавок:")
            bads = [
                item for item in report
                if any(kw in (item.get('subjectName') or '').lower()
                       for kw in ['бад', 'добавк', 'витамин', 'бады'])
                or any(kw in (item.get('parentName') or '').lower()
                       for kw in ['бад', 'добавк', 'витамин', 'бады'])
            ]

            if bads:
                print(f"  Найдено категорий с БАДами: {len(bads)}")
                for item in bads[:5]:
                    print(f"  - {item.get('subjectName')} (parent: {item.get('parentName')})")
                    print(f"    FBS: {item.get('kgvpMarketplace')}%")
            else:
                print("  ⚠️ Категории с БАДами не найдены")

            # Трансформируем данные
            df = WBTransformer.transform_tariffs_commission(raw_data)
            print(f"\n📊 Трансформировано строк: {len(df)}")
            print(f"   Колонки: {list(df.columns)}")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return False


def test_search_report(connector: WBConnector) -> bool:
    """Тест метода отчёта по поисковым позициям"""
    print("\n" + "="*60)
    print("TEST 2: fetch_search_report")
    print("="*60)

    try:
        # Берём вчерашний день
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        print(f"\nЗапрос за дату: {yesterday}")

        raw_data = connector.fetch_search_report(
            start=yesterday,
            end=yesterday,
            limit=100,  # Ограничиваем для теста
            offset=0,
        )

        # Проверяем структуру
        data = raw_data.get("data", {})
        groups = data.get("groups", [])

        print(f"\n✅ Получено групп: {len(groups)}")

        if groups:
            # Считаем общее количество товаров
            total_items = sum(len(g.get("items", [])) for g in groups)
            print(f"   Всего товаров: {total_items}")

            # Показываем первую группу
            first_group = groups[0]
            print(f"\nПример группы:")
            print(f"  Subject: {first_group.get('subjectName')} (ID: {first_group.get('subjectId')})")
            print(f"  Brand: {first_group.get('brandName')}")
            print(f"  Товаров в группе: {len(first_group.get('items', []))}")

            # Показываем первый товар
            items = first_group.get("items", [])
            if items:
                item = items[0]
                print(f"\n  Первый товар:")
                print(f"    nmId: {item.get('nmId')}")
                print(f"    name: {item.get('name')[:50]}..." if item.get('name') else "    name: N/A")
                print(f"    avgPosition: {item.get('avgPosition')}")
                print(f"    visibility: {item.get('visibility')}")

            # Трансформируем
            df = WBTransformer.transform_search_report_groups(groups, yesterday, yesterday)
            print(f"\n📊 Трансформировано строк: {len(df)}")
            print(f"   Колонки: {list(df.columns)[:10]}...")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_product_search_texts(connector: WBConnector, nm_ids: list = None) -> bool:
    """Тест метода поисковых запросов по товарам"""
    print("\n" + "="*60)
    print("TEST 3: fetch_product_search_texts")
    print("="*60)

    try:
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        # Если nm_ids не переданы, пробуем получить из search_report
        if not nm_ids:
            print("\nПолучаем nm_ids из search_report...")
            sr_data = connector.fetch_search_report(
                start=yesterday,
                end=yesterday,
                limit=10,
            )
            groups = sr_data.get("data", {}).get("groups", [])
            nm_ids = []
            for g in groups:
                for item in g.get("items", []):
                    if item.get("nmId"):
                        nm_ids.append(item["nmId"])
                    if len(nm_ids) >= 5:
                        break
                if len(nm_ids) >= 5:
                    break

        if not nm_ids:
            print("⚠️ Не удалось получить nm_ids для теста")
            return False

        print(f"\nЗапрос за дату: {yesterday}")
        print(f"nm_ids для теста: {nm_ids[:5]}")

        raw_data = connector.fetch_product_search_texts(
            nm_ids=nm_ids[:5],
            start=yesterday,
            end=yesterday,
            limit=10,  # Топ-10 запросов
        )

        items = raw_data.get("data", {}).get("items", [])

        print(f"\n✅ Получено поисковых запросов: {len(items)}")

        if items:
            print("\nПример данных (первые 5):")
            for item in items[:5]:
                print(f"  - Запрос: '{item.get('text')}'")
                print(f"    nmId: {item.get('nmId')}, freq: {item.get('frequency')}, pos: {item.get('avgPosition')}")

            # Трансформируем
            df = WBTransformer.transform_product_search_texts(items, yesterday, yesterday)
            print(f"\n📊 Трансформировано строк: {len(df)}")
            print(f"   Колонки: {list(df.columns)[:10]}...")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция тестирования"""
    print("\n" + "="*60)
    print("   ТЕСТИРОВАНИЕ НОВЫХ МЕТОДОВ WB API")
    print("="*60)

    # Получаем ключ API
    api_key = os.getenv("WB_KEY")
    if not api_key:
        print("❌ Ошибка: WB_KEY не найден в .env")
        sys.exit(1)

    print(f"\n🔑 API ключ: {api_key[:20]}...")

    # Создаём коннектор
    connector = WBConnector(api_key=api_key)

    # Запускаем тесты
    results = {}

    results["tariffs_commission"] = test_tariffs_commission(connector)
    results["search_report"] = test_search_report(connector)
    results["product_search_texts"] = test_product_search_texts(connector)

    # Итоги
    print("\n" + "="*60)
    print("   ИТОГИ ТЕСТИРОВАНИЯ")
    print("="*60)

    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    print("\n")

    if all_passed:
        print("🎉 Все тесты прошли успешно!")
    else:
        print("⚠️ Некоторые тесты не прошли")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

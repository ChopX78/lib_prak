import pandas as pd
import re

# Регулярное выражение для RFID
rfid_pattern = re.compile(r"^304DB75F19600014[0-9A-F]{8}$", re.IGNORECASE)


def load_scanned_tags():
    """Загружает и фильтрует метки из файла"""
    with open("list.txt", encoding="utf-8") as f:
        return {
            line.strip().upper() for line in f
            if line.strip() and rfid_pattern.fullmatch(line.strip())
        }


def extract_registry_tags(df):
    """Извлекает RFID и описания из реестра"""
    # Преобразование данных
    df_str = df.apply(lambda x: x.astype(str).str.strip().str.upper())

    # Поиск RFID
    rfid_mask = df_str.apply(lambda col: col.str.match(rfid_pattern))
    rfid_count = rfid_mask.sum(axis=1)

    # Индексы строк
    valid_indices = rfid_count[rfid_count == 1].index
    multiple_indices = rfid_count[rfid_count > 1].index
    empty_indices = rfid_count[(rfid_count == 0) & (df_str.ne('').any(axis=1))].index

    # Извлечение валидных RFID
    valid_rfids = df_str[rfid_mask].stack().reset_index(level=1, drop=True)
    valid_rfids = valid_rfids[~valid_rfids.index.duplicated(keep='first')]

    # Формирование описаний
    def get_description(row):
        rfid = valid_rfids.get(row.name)
        return ' | '.join([str(cell) for cell in row if str(cell) != rfid])

    descriptions = df_str.loc[valid_indices].apply(get_description, axis=1)

    tags = pd.Series(descriptions.values, index=valid_rfids.values).to_dict()

    # Формирование отчетов об ошибках
    bad_rows = []
    for idx in multiple_indices:
        row = df_str.loc[idx].tolist()
        bad_rows.append(["Строка", idx + 1] + row)
    for idx in empty_indices:
        row = df_str.loc[idx].tolist()
        bad_rows.append(["Строка", idx + 1] + row)

    if bad_rows:
        pd.DataFrame(bad_rows).to_excel("bad_registry_rows.xlsx", index=False, header=False)

    return tags


def extract_on_hands_tags(sheets):
    """Обрабатывает данные о книгах на руках"""
    bad_cells = []
    on_hands_tags = set()

    for sheet_name, df in sheets.items():
        # Преобразование данных в строки и очистка
        df_str = df.apply(lambda x: x.astype(str).str.strip().str.upper())

        # Поиск RFID
        rfid_mask = df_str.apply(lambda col: col.str.fullmatch(rfid_pattern))
        found_rfids = df_str[rfid_mask].stack().tolist()  # Конвертируем в список

        # Обновление множества RFID
        on_hands_tags.update(rfid.upper() for rfid in found_rfids)

        # Поиск строк без RFID
        has_rfid = rfid_mask.any(axis=1)
        bad_data = df_str[~has_rfid].apply(
            lambda row: ' | '.join(filter(None, row)),
            axis=1
        )

        # Сбор некорректных строк
        for idx, content in bad_data[bad_data != ''].items():
            bad_cells.append([
                "Лист", sheet_name,
                "Строка", idx + 1,
                "Содержимое", content
            ])

    # Сохранение ошибок
    if bad_cells:
        pd.DataFrame(bad_cells).to_excel("bad_on_hands.xlsx", index=False, header=False)

    return on_hands_tags


# Загрузка данных
scanned_tags = load_scanned_tags()
df_registry = pd.read_csv("Б7.csv", header=None, dtype=str)
registry_tags = extract_registry_tags(df_registry)

# Обработка данных о книгах
df_books = pd.read_excel("Книги на руках Б7.xlsx", sheet_name=None, header=None, dtype=str)
on_hands_tags = extract_on_hands_tags(df_books)

# Анализ данных
registry_set = set(registry_tags.keys())
real_on_hands = on_hands_tags & registry_set
missing_tags = registry_set - scanned_tags - real_on_hands

# Формирование отчетов
pd.DataFrame(
    [{"RFID": t, "Описание": registry_tags[t]} for t in sorted(missing_tags)]
).to_excel("missing_books.xlsx", index=False)

print(f"""\nИтоги:
Всего строк в реестре:               {len(df_registry)}
RFID в реестре (валидных):           {len(registry_set)}
Считано сканером:                    {len(scanned_tags)}
На руках (всего/из реестра):         {len(on_hands_tags)}/{len(real_on_hands)}
Недостающие экземпляры:              {len(missing_tags)}""")
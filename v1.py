import pandas as pd
import re

# регулярка для rfid
rfid_pattern = re.compile(r"304DB75F19600014[A-F0-9]{8}", re.IGNORECASE)

# загрузка файла с метками
with open("list.txt", encoding="utf-8") as f:
    scanned_tags = set(line.strip() for line in f if line.strip())
    scanned_tags = {t for t in scanned_tags if rfid_pattern.fullmatch(t)}

# загрузка общего реестра
df_registry = pd.read_csv("Б7.csv", header=None, encoding="utf-8", low_memory=False)

# извлечение меток из общего реестра
def extract_registry_tags(df):
    tags = {}
    multiple_tags_rows = []
    empty_rows = []

    for idx, row in df.iterrows():
        row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
        found_rfids = [val for val in row_values if rfid_pattern.fullmatch(val)]

        if len(found_rfids) == 1:
            rfid = found_rfids[0]
            description = " | ".join(x for x in row_values if x != rfid)
            tags[rfid] = description
        elif len(found_rfids) > 1:
            multiple_tags_rows.append(["Строка", idx + 1] + row_values)
        elif len(found_rfids) == 0 and row_values:
            empty_rows.append(["Строка", idx + 1] + row_values)

    # логирование некорректных строк в отдельную таблицу
    bad_rows = multiple_tags_rows + empty_rows
    if bad_rows:
        df_bad = pd.DataFrame(bad_rows)
        df_bad.to_excel("bad_registry_rows.xlsx", header=False, index=False, engine="openpyxl")
        print(f"\nСохранено {len(bad_rows)} некорректных строк в: bad_registry_rows.xlsx")

    return tags

registry_tags = extract_registry_tags(df_registry)

# загрузка файла с выданными книгами
def extract_on_hands_tags(excel_sheets):
    on_hands_tags = set()
    bad_cells = []

    for sheet_name, sheet_df in excel_sheets.items():
        for row_idx, row in sheet_df.iterrows():
            row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
            found_rfids = [val for val in row_values if rfid_pattern.fullmatch(val)]

            if found_rfids:
                on_hands_tags.update(found_rfids)
            elif row_values:
                # Сборка строк без rfid
                bad_cells.append([
                    "Лист", sheet_name,
                    "Строка", row_idx + 1,
                    "Столбцы", len(row),
                    "Содержимое", " | ".join(row_values)
                ])

    # Записываем строки без rfid
    if bad_cells:
        df_bad = pd.DataFrame(bad_cells)
        df_bad.to_excel("bad_on_hands.xlsx", header=False, index=False, engine="openpyxl")
        print(f"Сохранено {len(bad_cells)} некорректных строк в: bad_on_hands.xlsx")

    return on_hands_tags

df_excel = pd.read_excel("Книги на руках Б7.xlsx", sheet_name=None, header=None)
on_hands_tags = extract_on_hands_tags(df_excel)

# вычисление отсутствующих книг
registry_set = set(registry_tags.keys())
real_on_hands = on_hands_tags & registry_set
missing_tags = registry_set - scanned_tags - real_on_hands

# формирование вывода
missing_data = [{
    "RFID": tag,
    "Описание": registry_tags.get(tag, "")
} for tag in sorted(missing_tags)]

df_missing = pd.DataFrame(missing_data)

print("\nИтоги:")
print("Всего строк в реестре:               ", len(df_registry))
print("RFID в реестре (валидных):           ", len(registry_set))
print("Считано сканером:                    ", len(scanned_tags))
print("RFID на руках (всего):               ", len(on_hands_tags))
print("На руках, которые записаны в реестр: ", len(real_on_hands))
print("Недостающие экземпляры:              ", len(missing_tags))

# сохранение в файл
df_missing.to_excel("missing_books.xlsx", index=False, engine="openpyxl")
print("\n✅ Сохранено в файл: missing_books.xlsx")

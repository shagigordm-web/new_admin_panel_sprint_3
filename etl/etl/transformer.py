from typing import List, Dict, Any, Optional

def clean_value(value: Any) -> Optional[str]:
    """Очищает значение: приводит к строке и заменяет 'N/A' и 'None' на None."""
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == "N/A" or value.strip() == "None":
            return None
        return value.strip()
    # Если значение не строка, попробуем привести к строке
    try:
        str_val = str(value)
        if str_val.strip() == "N/A" or str_val.strip() == "None":
            return None
        return str_val.strip()
    except:
        return None

def parse_person_list(raw_list: Any) -> List[Dict[str, str]]:
    """Парсит список персонажей из формата 'id###name'. Возвращает список словарей."""
    # Убедимся, что raw_list - это список
    if not raw_list or not isinstance(raw_list, list):
        return []

    persons = []
    for item in raw_list:
        # Проверим, что элемент - строка
        if not item or not isinstance(item, str):
            continue
        item_str = clean_value(item)
        if not item_str:
            continue
        parts = item_str.split("###", 2)
        if len(parts) == 2:
            pid, name = parts
            name_clean = clean_value(name)
            if name_clean:  # Только если имя не N/A, не 'None' и не пустое
                persons.append({"id": pid.strip(), "name": name_clean})
    return persons

def transform_movies(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    movies = []
    for row in rows:
        # Парсим персонажей
        # parse_person_list гарантирует возврат списка
        actors = parse_person_list(row.get("actors"))
        writers = parse_person_list(row.get("writers"))
        directors = parse_person_list(row.get("directors"))

        # Обрабатываем жанры: оставляем только не-null, не-N/A, не-'None' строки
        genres_raw = row.get("genres") or []
        genres = []
        for g in genres_raw:
            g_clean = clean_value(g)
            if g_clean is not None:
                genres.append(g_clean)

        # Очищаем title и description
        title = clean_value(row.get("title"))
        description = clean_value(row.get("description"))

        # imdb_rating: преобразуем в float, если возможно, иначе None
        imdb_rating = None
        rating_val = row.get("imdb_rating")
        if rating_val is not None:
            # Проверим, может ли строка быть преобразована в float
            try:
                # Явно приведем к float, если возможно
                val = float(rating_val)
                # Убедимся, что значение не NaN и не infinity
                if not (val != val or val == float('inf') or val == float('-inf')):
                    imdb_rating = val
            except (ValueError, TypeError):
                # Если не получилось, оставим None
                imdb_rating = None

        # Формируем итоговый документ
        movie = {
            "id": str(row["id"]),  #id строка
            "imdb_rating": imdb_rating,
            "genres": genres, # genres - всегда список строк
            "title": title,
            "description": description,
            "directors_names": [p["name"] for p in directors], # directors_names - всегда список строк
            "actors_names": [p["name"] for p in actors], # actors_names - всегда список строк
            "writers_names": [p["name"] for p in writers], # writers_names - всегда список строк
            "directors": directors, # directors - всегда список объектов {"id": "...", "name": "..."}
            "actors": actors, # actors - всегда список объектов {"id": "...", "name": "..."}
            "writers": writers, # writers - всегда список объектов {"id": "...", "name": "..."}
        }

        movies.append(movie)

    return movies
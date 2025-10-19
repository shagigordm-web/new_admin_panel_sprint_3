# Запрос для извлечения фильмов, измененных после указанной даты
EXTRACT_FILMWORK_QUERY = """
SELECT
    fw.id,
    fw.title,
    fw.description,
    fw.rating AS imdb_rating,
    fw.modified AS modified,
    ARRAY_AGG(DISTINCT g.name) FILTER (WHERE g.name IS NOT NULL) AS genres,
    ARRAY_AGG(DISTINCT p.id || '###' || p.full_name) FILTER (WHERE pfw.role = 'actor' AND p.full_name IS NOT NULL) AS actors,
    ARRAY_AGG(DISTINCT p.id || '###' || p.full_name) FILTER (WHERE pfw.role = 'writer' AND p.full_name IS NOT NULL) AS writers,
    ARRAY_AGG(DISTINCT p.id || '###' || p.full_name) FILTER (WHERE pfw.role = 'director' AND p.full_name IS NOT NULL) AS directors
FROM content."film_work" fw
LEFT JOIN content."genre_film_work" gfw ON fw.id = gfw.film_work_id
LEFT JOIN content."genre" g ON gfw.genre_id = g.id
LEFT JOIN content."person_film_work" pfw ON fw.id = pfw.film_work_id
LEFT JOIN content."person" p ON pfw.person_id = p.id
WHERE fw.modified > %s
GROUP BY fw.id
ORDER BY fw.modified
LIMIT %s;
"""

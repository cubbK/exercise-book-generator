{{ config(
    materialized='table',
    schema='gold',
) }}

SELECT
  books.title                 AS book_title,
  books.book_id               AS book_id,
  chapters.chapter_id         AS chapter_id,
  chapters.chapter_order      AS chapter_order,
  chapters.raw_text,
  categories.category,
  a2_text.a2_text,
  a2_summary.summary          AS a2_summary,
  b1b2_text.b1b2_text,
  swedish_text.swedish_text,
  chapter_lang.detected_language AS source_language
FROM
  {{ source('silver', 'chapters') }} AS chapters
LEFT JOIN
  {{ source('silver', 'books') }} AS books
  ON chapters.book_id = books.book_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_categories') }} AS categories
  ON chapters.chapter_id = categories.chapter_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_swedish_a2_text') }} AS a2_text
  ON chapters.chapter_id = a2_text.chapter_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_swedish_a2_summary') }} AS a2_summary
  ON chapters.chapter_id = a2_summary.chapter_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_swedish_b1b2_text') }} AS b1b2_text
  ON chapters.chapter_id = b1b2_text.chapter_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_swedish_text') }} AS swedish_text
  ON chapters.chapter_id = swedish_text.chapter_id
LEFT JOIN
  {{ source('gold_llm', 'chapter_language') }} AS chapter_lang
  ON chapters.chapter_id = chapter_lang.chapter_id

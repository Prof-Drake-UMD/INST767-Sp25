-- sql/analytical_queries.sql

-- Query 1: Detailed profiles of books that have appeared on an NYT Bestseller list
-- Provides a 360-degree view of NYT bestselling books by combining data from all three sources.
SELECT
    nb.title AS nyt_title,
    nb.author AS nyt_author,
    nb.list_name AS nyt_list_name,
    nb.rank AS nyt_rank,
    nb.weeks_on_list AS nyt_weeks_on_list,
    nb.bestseller_date AS nyt_bestseller_date,
    olb.title AS ol_title,
    olb.authors AS ol_authors,
    olb.subjects AS ol_subjects,
    olb.number_of_pages AS ol_pages,
    olb.publish_places AS ol_publish_places,
    olb.publish_date AS ol_publish_date,
    olb.publishers AS ol_publishers,
    gbd.title AS gb_title,
    gbd.authors AS gb_authors,
    gbd.average_rating AS gb_average_rating,
    gbd.ratings_count AS gb_ratings_count,
    COALESCE(NULLIF(gbd.page_count, 0), NULLIF(olb.number_of_pages, 0)) AS best_guess_page_count,
    gbd.categories AS gb_categories,
    gbd.description AS gb_description,
    gbd.thumbnail_link AS gb_thumbnail_link,
    gbd.saleability AS gb_saleability,
    gbd.list_price_amount AS gb_list_price,
    gbd.list_price_currency_code AS gb_list_price_currency,
    gbd.sale_info_json,
    gbd.access_info
FROM
    `realtime-book-data-gcp.mohammed_alazem_dataset.nyt_books` nb
LEFT JOIN
    `realtime-book-data-gcp.mohammed_alazem_dataset.open_library_books` olb ON nb.isbn13 = olb.isbn13
LEFT JOIN
    `realtime-book-data-gcp.mohammed_alazem_dataset.google_books_details` gbd ON nb.isbn13 = gbd.isbn13
ORDER BY
    nb.bestseller_date DESC, nb.rank ASC
LIMIT 100;


-- Query 2: Emerging NYT bestselling authors and their broader work
-- Identifies authors who recently hit the NYT bestseller list (e.g., 1 week on list)
-- Output is grouped to ensure distinct rows based on emerging author and their defining NYT book.
WITH EmergingNYTBooks AS (
    SELECT
        author,
        title AS nyt_title,
        isbn13 AS nyt_isbn13, -- Select NYT ISBN13
        bestseller_date,
        ROW_NUMBER() OVER(PARTITION BY author ORDER BY bestseller_date ASC, title ASC) as rn
    FROM
        `realtime-book-data-gcp.mohammed_alazem_dataset.nyt_books`
    WHERE
        weeks_on_list = 1
),
EmergingAuthors AS (
    SELECT
        author,
        nyt_title,
        nyt_isbn13, -- Pass NYT ISBN13
        bestseller_date AS first_bestseller_date
    FROM EmergingNYTBooks
    WHERE rn = 1
)
SELECT
    ea.author AS emerging_author,
    ea.nyt_isbn13, -- Display NYT ISBN13
    ea.first_bestseller_date,
    COALESCE(ea.nyt_title, ol_books.title, gb_books.title) AS book_title,
    ol_books.publish_date AS ol_publish_date,
    ol_books.subjects AS ol_subjects,
    gb_books.published_date AS gb_published_date,
    gb_books.categories AS gb_categories,
    gb_books.average_rating AS gb_average_rating,
    gb_books.ratings_count AS gb_ratings_count
FROM
    EmergingAuthors ea
LEFT JOIN
    (SELECT
        title, publish_date, subjects, author_name
     FROM (
        SELECT
            ol.title, ol.publish_date, ol.subjects,
            author_name, 
            ROW_NUMBER() OVER(PARTITION BY author_name ORDER BY ol.publish_date DESC, ol.title ASC) as rn_ol
        FROM `realtime-book-data-gcp.mohammed_alazem_dataset.open_library_books` ol,
             UNNEST(ol.authors) AS author_name
        )
     WHERE rn_ol = 1
    ) AS ol_books ON ea.author = ol_books.author_name
LEFT JOIN
    (SELECT
        title, published_date, categories, average_rating, ratings_count, author_name
     FROM (
        SELECT
            gb.title, gb.published_date, gb.categories, gb.average_rating, gb.ratings_count,
            author_name, 
            ROW_NUMBER() OVER(PARTITION BY author_name ORDER BY gb.published_date DESC, gb.title ASC) as rn_gb
        FROM `realtime-book-data-gcp.mohammed_alazem_dataset.google_books_details` gb,
             UNNEST(gb.authors) AS author_name
        )
     WHERE rn_gb = 1
    ) AS gb_books ON ea.author = gb_books.author_name
GROUP BY -- Group by all selected columns
    ea.author,
    ea.nyt_isbn13,
    ea.first_bestseller_date,
    book_title, 
    ol_publish_date,
    ol_subjects,
    gb_published_date,
    gb_categories,
    gb_average_rating,
    gb_ratings_count
ORDER BY
    ea.author, ol_books.publish_date DESC, gb_books.published_date DESC
LIMIT 200;


-- Query 3: Availability and Pricing Insights for NYT Bestsellers
-- Analyzes market dynamics for NYT bestsellers focusing on list price, buy links, and digital access information from Google Books.
SELECT
    nb.title AS nyt_title,
    nb.author AS nyt_author,
    nb.rank AS nyt_rank,
    nb.weeks_on_list AS nyt_weeks_on_list,
    gbd.isbn13,
    gbd.saleability AS gb_saleability,
    gbd.list_price_amount AS gb_list_price,
    gbd.list_price_currency_code AS gb_list_price_currency,
    gbd.buy_link AS gb_buy_link,
    JSON_EXTRACT_SCALAR(gbd.access_info, '$.viewability') AS gb_viewability,
    JSON_EXTRACT_SCALAR(gbd.access_info, '$.epub.isAvailable') AS gb_epub_available,
    JSON_EXTRACT_SCALAR(gbd.access_info, '$.pdf.isAvailable') AS gb_pdf_available,
    gbd.access_info AS gb_access_info_json -- Keep for reference
FROM
    `realtime-book-data-gcp.mohammed_alazem_dataset.nyt_books` nb
INNER JOIN
    `realtime-book-data-gcp.mohammed_alazem_dataset.google_books_details` gbd ON nb.isbn13 = gbd.isbn13
WHERE
    -- Ensure there's relevant commercial or access info: has a list price OR a buy link OR EPUB is available
    (
        gbd.list_price_amount IS NOT NULL OR
        gbd.buy_link IS NOT NULL OR
        JSON_EXTRACT_SCALAR(gbd.access_info, '$.epub.isAvailable') = 'true'
    )
ORDER BY
    nb.bestseller_date DESC, nb.rank ASC
LIMIT 100;


-- Query 4: Characteristics and trends within a specific book genre (e.g., "Fiction")
-- Deep dive into books categorized as 'Fiction' in Google Books.
SELECT
    gbd.title,
    gbd.authors,
    gbd.publisher,
    gbd.published_date,
    COALESCE(NULLIF(gbd.page_count, 0), NULLIF(olb.number_of_pages, 0)) AS page_count,
    gbd.average_rating,
    gbd.ratings_count,
    olb.subjects AS ol_subjects,
    nb.rank AS nyt_rank, -- If it was an NYT bestseller
    nb.weeks_on_list AS nyt_weeks_on_list -- If it was an NYT bestseller
FROM
    `realtime-book-data-gcp.mohammed_alazem_dataset.google_books_details` gbd
LEFT JOIN
    `realtime-book-data-gcp.mohammed_alazem_dataset.open_library_books` olb ON gbd.isbn13 = olb.isbn13
LEFT JOIN
    `realtime-book-data-gcp.mohammed_alazem_dataset.nyt_books` nb ON gbd.isbn13 = nb.isbn13
WHERE
    EXISTS (SELECT 1 FROM UNNEST(gbd.categories) AS category WHERE LOWER(category) LIKE '%fiction%')
ORDER BY
    gbd.average_rating DESC, gbd.ratings_count DESC
LIMIT 100;


-- Query 5: Correlation between book length and commercial success (NYT weeks on list) or reader ratings
-- Groups books by page count ranges to see average weeks on list and ratings.
WITH BookStats AS (
    SELECT
        gbd.isbn13,
        COALESCE(NULLIF(gbd.page_count, 0), NULLIF(olb.number_of_pages, 0)) AS pages,
        MAX(nb.weeks_on_list) AS max_weeks_on_list, -- Max weeks if it appeared multiple times
        gbd.average_rating,
        gbd.ratings_count
    FROM
        `realtime-book-data-gcp.mohammed_alazem_dataset.google_books_details` gbd
    LEFT JOIN
        `realtime-book-data-gcp.mohammed_alazem_dataset.open_library_books` olb ON gbd.isbn13 = olb.isbn13
    LEFT JOIN
        `realtime-book-data-gcp.mohammed_alazem_dataset.nyt_books` nb ON gbd.isbn13 = nb.isbn13
    WHERE
        COALESCE(NULLIF(gbd.page_count, 0), NULLIF(olb.number_of_pages, 0)) IS NOT NULL
    GROUP BY
        gbd.isbn13,
        pages, -- Group by the alias 'pages'
        gbd.average_rating,
        gbd.ratings_count
)
SELECT
    CASE
        WHEN pages <= 100 THEN '0-100'
        WHEN pages <= 200 THEN '101-200'
        WHEN pages <= 300 THEN '201-300'
        WHEN pages <= 400 THEN '301-400'
        WHEN pages <= 500 THEN '401-500'
        ELSE '>500'
    END AS page_range,
    COUNT(DISTINCT isbn13) AS number_of_books,
    AVG(max_weeks_on_list) AS avg_nyt_weeks_on_list,
    AVG(average_rating) AS avg_google_rating,
    SUM(ratings_count) AS total_google_ratings_count
FROM
    BookStats
GROUP BY
    page_range
ORDER BY
    page_range;


-- Query 6: Newcomers vs. Veterans on Bestseller List - Rating & Page Count Comparison
-- This query analyzes how new books on the bestseller list compare to established ones
-- within each Google Books category. It looks at:
-- 1. Average ratings for newcomers (top 3 newest) vs veterans (rest)
-- 2. Average page counts for both groups (using Google Books or Open Library data)
-- 3. Only includes categories with more than 5 books for statistical significance
-- Not enough data to make this query useful yet
WITH CategorizedBestsellers AS (
    SELECT
        n.title,
        n.author,
        n.weeks_on_list,
        COALESCE(NULLIF(g.page_count, 0), NULLIF(ol.number_of_pages, 0)) AS page_count,
        g.average_rating,
        g.categories,
        cat AS google_category,
        ROW_NUMBER() OVER (PARTITION BY cat ORDER BY n.weeks_on_list ASC) AS rank_in_category_by_weeks
    FROM
        mohammed_alazem_dataset.nyt_books AS n
    JOIN
        mohammed_alazem_dataset.google_books_details AS g ON n.isbn13 = g.isbn13
    LEFT JOIN
        mohammed_alazem_dataset.open_library_books AS ol ON n.isbn13 = ol.isbn13,
        UNNEST(g.categories) AS cat
    WHERE 
        g.average_rating IS NOT NULL 
        AND COALESCE(NULLIF(g.page_count, 0), NULLIF(ol.number_of_pages, 0)) IS NOT NULL
        AND ARRAY_LENGTH(g.categories) > 0
)
SELECT
    google_category,
    AVG(CASE WHEN rank_in_category_by_weeks <= 3 THEN average_rating END) AS avg_rating_newcomers,
    AVG(CASE WHEN rank_in_category_by_weeks > 3 THEN average_rating END) AS avg_rating_veterans,
    AVG(CASE WHEN rank_in_category_by_weeks <= 3 THEN page_count END) AS avg_pages_newcomers,
    AVG(CASE WHEN rank_in_category_by_weeks > 3 THEN page_count END) AS avg_pages_veterans,
    COUNT(DISTINCT title) as num_books_in_category
FROM
    CategorizedBestsellers
GROUP BY 
    google_category
HAVING 
    COUNT(DISTINCT title) > 1
ORDER BY 
    num_books_in_category DESC, 
    google_category; 
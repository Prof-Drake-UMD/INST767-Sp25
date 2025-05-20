CREATE DATABASE my_project_db;
USE my_project_db;

-- Table for CheapShark Game Deals
CREATE TABLE game_deals (
    title VARCHAR(100),
    normal_price DECIMAL(5,2),
    sale_price DECIMAL(5,2),
    savings DECIMAL(5,2),
    steam_rating VARCHAR(10),
    store_id VARCHAR(10),
    deal_url TEXT
);

-- Table for Magic: The Gathering Cards
CREATE TABLE mtg_cards (
    name VARCHAR(100),
    mana_cost VARCHAR(20),
    cmc INT,
    type VARCHAR(100),
    colors VARCHAR(50),
    power VARCHAR(10),
    toughness VARCHAR(10),
    rarity VARCHAR(20),
    set_name VARCHAR(50),
    image_url TEXT
);

-- Table for Disney Characters
CREATE TABLE disney_characters (
    name VARCHAR(100),
    films TEXT,
    tv_shows TEXT,
    video_games TEXT,
    source_url TEXT,
    image_url TEXT
);
-- 1. Find top 5 best-value game deals based on savings and Steam rating
SELECT 
    title, 
    normal_price, 
    sale_price, 
    savings, 
    steam_rating,
    ROUND((savings * IFNULL(steam_rating, 50)) / 100, 2) AS value_score,
    deal_url
FROM game_deals
WHERE 
    savings IS NOT NULL 
    AND steam_rating IS NOT NULL
ORDER BY value_score DESC
LIMIT 5;

-- 2. Count how many MTG cards exist for each rarity
SELECT 
    rarity, 
    COUNT(*) AS count
FROM mtg_cards
GROUP BY rarity
ORDER BY count DESC;

-- 3. Find Disney characters who appear in both TV shows and video games
SELECT 
    name
FROM disney_characters
WHERE 
    tv_shows IS NOT NULL AND tv_shows != ''
    AND video_games IS NOT NULL AND video_games != '';

-- 4. Get average power and toughness for MTG cards (numeric only)
SELECT 
    AVG(CAST(power AS UNSIGNED)) AS avg_power,
    AVG(CAST(toughness AS UNSIGNED)) AS avg_toughness
FROM mtg_cards
WHERE 
    power REGEXP '^[0-9]+$' 
    AND toughness REGEXP '^[0-9]+$';

-- 5. Count how many game deals each store has
SELECT 
    store_id, 
    COUNT(*) AS total_deals
FROM game_deals
GROUP BY store_id
ORDER BY total_deals DESC;


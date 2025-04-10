# SQL Query Results from BigQuery

This folder contains various SQL queries executed on **Google BigQuery**, along with their respective output screenshots stored in the `files` directory.

---

## 📁 Structure

- `interesting_queries.sql` – Complex or notable queries
- `news_table.sql` – Queries related to the news dataset
- `files/` – Contains images of query results and visualizations

---

## 📸 Query Results

### 1. Query: Top 10 Most Positive Sources

![Top News Sources](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/top_news_sources.png)

![Result_1](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/result_1.png)

_This query retrieves the top 10 news sources based on the number of articles published._

---

### 2. Query: Sentiment Trend Over Time

![Average Sentiment](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/avg_article_length.png)

![Result_2](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/result_2.png)

_This query calculates the average daily sentiment of the articles._

---

### 3. Query: Most Negative Headlines in the Past 7 days

![Negative_headlines](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/most_negative.png)

![Result_3](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/result_3.png)

_This query identifies the most negative news artilces over the past 7 days._

---

### 4. Query: Sources with the Highest Sentiment Volatility

![highest-volatility](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/highest_volatility.png)

![Result_4](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/result_4.png)

_This query identifies the most negative news artilces over the past 7 days._

---

### 5. Query: Daily Article Volume by Source

![article_volume](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/daily_volume.png)

![Result_5](https://github.com/Prof-Drake-UMD/INST767-Sp25/blob/4eb9da4f2ab6c1ec5cc5de02839543ad2ac950a7/Robin_Godinho/sql/files/result_5.png)

_This query tracks the daily article volume by source._

---

## 💡 Notes

- All queries were written and executed in **BigQuery**.
- Images are exported results or visual snapshots from the BigQuery UI.

---

## 📂 How to Add More

1. Add your query to a `.sql` file.
2. Run it in BigQuery and download the result as an image or take a screenshot.
3. Save it in the `files` folder.
4. Update this `README.md` using the format above.


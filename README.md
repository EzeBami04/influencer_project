#  Influencer Discovery and Cross-Platform Matching Pipeline

This project automates the **discovery of influencers** by searching for Instagram profiles using Google Search, validating them based on follower counts, and finding matching profiles across **YouTube, TikTok, and X (Twitter)**.  
All discovered usernames are then stored in a **PostgreSQL database** for further analysis or business intelligence workflows.

---

##  Overview

### Pipeline Flow

1. **Keyword-based discovery**  
   Uses Google Search (`site:instagram.com`) to find public Instagram profiles related to specific keywords (e.g., *comedian, blogger, fashion, artist*).

2. **Follower extraction & filtering**  
   Parses search snippets and estimates follower counts using pattern recognition (e.g., `2.5M followers → 2500000`).

3. **Cross-platform enrichment**  
   For each valid Instagram username, the search uses both webscraping and AI solutions to match the the discovered instagram name to look up the other platforms using the following urls for all the platform:
   - YouTube: Searches for `site:youtube.com/@username`
   - TikTok: Searches for `site:tiktok.com/@username`
   - X (Twitter): Visits `x.com/{username}` to verify handle existence


4. **Data persistence**  
   Results are saved to a PostgreSQL database table `username_search` using an upsert logic, containing:
   - Tiktok username matched with the instagram username in the event where there  is no match the table defaults to null
   - instagram username
   - youtube channel matched with the instagram username
   - X username matched with the username


5. **Downstream pipelines**
 The solution also features downstream pipelines which takes usernames from the `user_search` table and load collected from either respective APIs or webscraping as occasion permits the data into the postgres database seperating the tables into both post and user data with the relevant platform user_id as the existing relationships.

6. **CI/CD** 
   GitAction remain the sole CI solution for this project with each of the independent pipelines triggered from the git workflow
---

## 🧠 Key Features

 - Asynchronous execution with **`asyncio`** for parallel profile discovery  
 -  **Playwright + BeautifulSoup4** integration for scraping and parsing  
 - Proxy rotation support for Google Search stealth mode  
 - Follower count normalization and influencer filtering  
 - Robust **PostgreSQL data persistence** via `psycopg2`  
 - Configurable keyword lists and concurrency limits  
 - Auto table creation (`username_search`) on first run  

---

## 📦 Tech Stack

| Component | Purpose |
|------------|----------|
| **Python 3.10+** | Core runtime |
| **Playwright (async)** | Browser automation for YouTube, TikTok, X |
| **httpx (async)** | Proxy-based async HTTP requests |
| **BeautifulSoup4** | HTML parsing and username extraction |
| **psycopg2** | PostgreSQL database connectivity |
| **dotenv** | Environment variable management |
| **logging** | Unified async logging and monitoring |

---

## ⚙️ Environment Setup
###  Clone the repository
```bash
git clone https://github.com/influencer-dashboard/influencer_project.git
cd influencer_project

```
### set up Python environment
``` bash
python -m venv environment-name

```

### Install dependencies
``` bash
pip install -r requirements.txt
```

### Scripts
``` bash
python script-name.py
```





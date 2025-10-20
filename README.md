# 🌍 Influencer Discovery & Cross-Platform Matching Pipeline

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
   For each valid Instagram username:
   - YouTube: Searches for `site:youtube.com/@username`
   - TikTok: Searches for `site:tiktok.com/@username`
   - X (Twitter): Visits `x.com/{username}` to verify handle existence

4. **Data persistence**  
   Results are saved to a PostgreSQL database table `username_search`, containing:



---

## 🧠 Key Features

✅ Asynchronous execution with **`asyncio`** for parallel profile discovery  
✅ **Playwright + BeautifulSoup4** integration for scraping and parsing  
✅ Proxy rotation support for Google Search stealth mode  
✅ Follower count normalization and influencer filtering  
✅ Robust **PostgreSQL data persistence** via `psycopg2`  
✅ Configurable keyword lists and concurrency limits  
✅ Auto table creation (`username_search`) on first run  

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

### 1️⃣ Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/influencer_project.git
cd influencer_project



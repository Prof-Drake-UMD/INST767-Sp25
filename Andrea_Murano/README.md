# Cozy Day Curator

## Project Description: As a lover of the great indoors, I feel particularly invested in, and equipped to, create the Cozy Day Curator. This recommendation system is perfect for those who enjoy a quiet day at home, offering book suggestions, potential activities, and even a soundtrack to score what is sure to be a perfect day! This system provides peak relation, because you don't even have to make decisions for yourself - Cozy Day Curator will do it for you.

## APIs Used:
1. **Pinterest API:** Low-key crafts or a new recipe for that special night alone! 
2. **Open Library API:** Books so that you can read about people and not actually have to be around them.
3. **LastFM API:** Curated playlists to set the mood.

## How to Run
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Prof-Drake-UMD/INST767-Sp25.git
   cd INST767-Sp25
   ```
2. **Install Dependencies:**
   Install the required Python libraries:
   ```bash
   pip install -r requirements.txt
   ```
3. **Set Up API Keys:**
   - Create a `.env` file in the root directory.
   - Use the provided `.env.example` as a template to add your API keys:
     ```
     LASTFM_API_KEY=your_lastfm_api_key
     OPEN_LIBRARY_API_KEY=your_open_library_api_key
     PINTEREST_API_KEY=your_pinterest_api_key
     ```
4. **Run the Program:**
   Execute the application:
   ```bash
   python main.py
   ```
5. **Enjoy Your Recommendations:**
   Sit back, relax, and let the Cozy Day Curator plan your perfect day!

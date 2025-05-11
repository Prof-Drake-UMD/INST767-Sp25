# "Culture Yourself" Curator

## Project Description: Generate a work of art, a book, and a song to expand your cultural horizons. The goal is that each element will be connected by a theme.

## APIs Used:
1. **The MET Collection API:** Access to over 450,000 works of art.
2. **Open Library API:** Access to millions of books and bibliographic metadata. 
3. **TheAudioDB Free Music API:** Access metadata about music, including artist details, track listings, genres, moods, and album covers.

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

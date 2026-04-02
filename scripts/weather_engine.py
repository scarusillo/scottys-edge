#!/usr/bin/env python3
"""
WEATHER ENGINE — Walters' outdoor sports edge factor.

Walters factored weather into every outdoor game. Wind, rain, and cold
push totals down. The market sometimes doesn't adjust fast enough,
especially for mid-week games and minor leagues.

Uses OpenWeatherMap free tier (1000 calls/day — more than enough).
Sign up at: https://openweathermap.org/api (free, takes 2 minutes)

Affects: baseball_ncaa, soccer (all leagues), MLS
Does NOT affect: NBA, NCAAB, NHL (indoor)

Usage:
    from weather_engine import get_weather_adjustment
    adj, info = get_weather_adjustment(venue_city, sport, commence_time)
"""
import os
import json
import math
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════
# API KEY — Set as environment variable or hardcode here
# ═══════════════════════════════════════════════════════════════════
# Get free key at: https://openweathermap.org/api
# Then: set OPENWEATHER_API_KEY=your_key_here (Windows)
# Or hardcode below:
API_KEY = os.environ.get('OPENWEATHER_API_KEY', '')


# ═══════════════════════════════════════════════════════════════════
# VENUE COORDINATES — lat/lon for home team stadiums
# ═══════════════════════════════════════════════════════════════════
# Only outdoor venues matter. Indoor sports skip weather entirely.

MLS_VENUES = {
    'Atlanta United FC': (33.755, -84.401),
    'Austin FC': (30.388, -97.722),
    'CF Montréal': (45.562, -73.553),
    'Charlotte FC': (35.206, -80.837),
    'Chicago Fire FC': (41.862, -87.617),
    'Colorado Rapids': (39.806, -104.892),
    'Columbus Crew': (39.969, -83.017),
    'D.C. United': (38.868, -77.013),
    'FC Cincinnati': (39.111, -84.522),
    'FC Dallas': (33.155, -96.835),
    'Houston Dynamo': (29.752, -95.354),
    'Inter Miami CF': (25.958, -80.239),
    'LA Galaxy': (33.864, -118.261),
    'LAFC': (34.012, -118.284),
    'Minnesota United FC': (44.953, -93.165),
    'Nashville SC': (36.130, -86.766),
    'New England Revolution': (42.091, -71.264),
    'New York City FC': (40.829, -73.926),
    'New York Red Bulls': (40.737, -74.150),
    'Orlando City SC': (28.541, -81.389),
    'Philadelphia Union': (39.833, -75.379),
    'Portland Timbers': (45.522, -122.687),
    'Real Salt Lake': (40.583, -111.893),
    'San Diego FC': (32.783, -117.150),
    'San Jose Earthquakes': (37.351, -121.925),
    'Seattle Sounders FC': (47.595, -122.332),
    'Sporting Kansas City': (39.122, -94.822),
    'St. Louis City SC': (38.631, -90.210),
    'Toronto FC': (43.633, -79.418),
    'Vancouver Whitecaps FC': (49.277, -123.110),
}

# EPL venues (all outdoor)
EPL_VENUES = {
    'Arsenal': (51.555, -0.108),
    'Aston Villa': (52.509, -1.885),
    'Bournemouth': (50.735, -1.838),
    'Brentford': (51.491, -0.289),
    'Brighton': (50.862, -0.084),
    'Chelsea': (51.482, -0.191),
    'Crystal Palace': (51.398, -0.086),
    'Everton': (53.439, -2.966),
    'Fulham': (51.475, -0.222),
    'Ipswich Town': (52.055, 1.145),
    'Leicester City': (52.620, -1.142),
    'Liverpool': (53.431, -2.961),
    'Manchester City': (53.483, -2.200),
    'Manchester United': (53.463, -2.291),
    'Newcastle United': (54.976, -1.622),
    'Nottingham Forest': (52.940, -1.133),
    'Southampton': (50.906, -1.391),
    'Tottenham Hotspur': (51.604, -0.066),
    'West Ham United': (51.539, 0.017),
    'Wolverhampton Wanderers': (52.590, -2.130),
}

# La Liga venues (all outdoor)
LA_LIGA_VENUES = {
    'Alavés': (42.837, -2.688), 'Athletic Bilbao': (43.264, -2.949),
    'Atletico Madrid': (40.436, -3.600), 'Barcelona': (41.381, 2.123),
    'CA Osasuna': (42.796, -1.637), 'Celta Vigo': (42.212, -8.740),
    'Elche CF': (38.267, -0.663), 'Espanyol': (41.348, 2.076),
    'Getafe': (40.326, -3.715), 'Girona': (41.961, 2.829),
    'Levante': (39.495, -0.354), 'Mallorca': (39.590, 2.630),
    'Oviedo': (43.364, -5.870), 'Rayo Vallecano': (40.392, -3.659),
    'Real Betis': (37.357, -5.982), 'Real Madrid': (40.453, -3.688),
    'Real Sociedad': (43.301, -1.974), 'Sevilla': (37.384, -5.970),
    'Valencia': (39.475, -0.358), 'Villarreal': (39.944, -0.104),
}

# Serie A venues (all outdoor)
SERIE_A_VENUES = {
    'AC Milan': (45.478, -9.124), 'AS Roma': (41.934, 12.455),
    'Atalanta': (45.709, 9.681), 'Bologna': (44.493, 11.310),
    'Cagliari': (39.200, 9.137), 'Como': (45.815, 9.076),
    'Cremonese': (45.137, 10.014), 'Empoli': (43.726, 10.955),
    'Fiorentina': (43.781, 11.282), 'Genoa': (44.416, 8.953),
    'Hellas Verona': (45.435, 10.969), 'Inter Milan': (45.478, -9.124),
    'Juventus': (45.110, 7.641), 'Lazio': (41.934, 12.455),
    'Lecce': (40.364, 18.189), 'Monza': (45.584, 9.279),
    'Napoli': (40.828, 14.193), 'Parma': (44.795, 10.338),
    'Pisa': (43.726, 10.408), 'Sassuolo': (44.715, 10.653),
    'Torino': (45.042, 7.649), 'Udinese': (46.082, 13.200),
    'Venezia': (45.456, 12.345),
}

# Bundesliga venues (all outdoor)
BUNDESLIGA_VENUES = {
    '1. FC Heidenheim': (48.676, 10.153), '1. FC Köln': (50.934, 6.875),
    'Augsburg': (48.323, 10.886), 'Bayer Leverkusen': (51.038, 7.002),
    'Bayern Munich': (48.219, 11.625), 'Borussia Dortmund': (51.493, 7.452),
    'Borussia Monchengladbach': (51.175, 6.386), 'Eintracht Frankfurt': (50.069, 8.645),
    'FC St. Pauli': (53.555, 9.968), 'FSV Mainz 05': (49.984, 8.224),
    'Hamburger SV': (53.587, 9.899), 'RB Leipzig': (51.346, 12.348),
    'SC Freiburg': (48.022, 7.830), 'TSG Hoffenheim': (49.239, 8.888),
    'Union Berlin': (52.457, 13.568), 'VfB Stuttgart': (48.792, 9.232),
    'VfL Wolfsburg': (52.432, 10.804), 'Werder Bremen': (53.066, 8.838),
}

# Ligue 1 venues (all outdoor)
LIGUE_1_VENUES = {
    'AJ Auxerre': (47.793, 3.588), 'Angers': (47.461, -0.530),
    'Brest': (48.384, -4.461), 'Le Havre': (49.499, 0.156),
    'Lens': (50.433, 2.815), 'Lille': (50.612, 3.130),
    'Lorient': (47.747, -3.370), 'Lyon': (45.765, 4.982),
    'Marseille': (43.270, 5.396), 'Metz': (49.110, 6.160),
    'AS Monaco': (43.728, 7.415), 'Montpellier': (43.622, 3.812),
    'Nantes': (47.256, -1.525), 'Nice': (43.705, 7.192),
    'Paris Saint Germain': (48.842, 2.253), 'RC Lens': (50.433, 2.815),
    'Reims': (49.247, 3.930), 'Rennes': (48.108, -1.713),
    'Saint-Etienne': (45.461, 4.390), 'Strasbourg': (48.560, 7.751),
    'Toulouse': (43.583, 1.435),
}

# MLB venues (all outdoor except Tropicana Field — included anyway, retractable roof)
MLB_VENUES = {
    'Arizona Diamondbacks': (33.4455, -112.0667),
    'Atlanta Braves': (33.8907, -84.4678),
    'Baltimore Orioles': (39.2838, -76.6216),
    'Boston Red Sox': (42.3467, -71.0972),
    'Chicago Cubs': (41.9484, -87.6553),
    'Chicago White Sox': (41.8299, -87.6338),
    'Cincinnati Reds': (39.0974, -84.5065),
    'Cleveland Guardians': (41.4962, -81.6852),
    'Colorado Rockies': (39.7559, -104.9942),
    'Detroit Tigers': (42.3390, -83.0485),
    'Houston Astros': (29.7573, -95.3555),
    'Kansas City Royals': (39.0517, -94.4803),
    'Los Angeles Angels': (33.8003, -117.8827),
    'Los Angeles Dodgers': (34.0739, -118.2400),
    'Miami Marlins': (25.7781, -80.2196),
    'Milwaukee Brewers': (43.0280, -87.9712),
    'Minnesota Twins': (44.9817, -93.2776),
    'New York Mets': (40.7571, -73.8458),
    'New York Yankees': (40.8296, -73.9262),
    'Oakland Athletics': (38.5802, -121.5064),
    'Philadelphia Phillies': (39.9061, -75.1665),
    'Pittsburgh Pirates': (40.4469, -80.0058),
    'San Diego Padres': (32.7076, -117.1570),
    'San Francisco Giants': (37.7786, -122.3893),
    'Seattle Mariners': (47.5914, -122.3325),
    'St. Louis Cardinals': (38.6226, -90.1928),
    'Tampa Bay Rays': (27.7682, -82.6534),
    'Texas Rangers': (32.7512, -97.0832),
    'Toronto Blue Jays': (43.6414, -79.3894),
    'Washington Nationals': (38.8730, -77.0074),
}

# ═══════════════════════════════════════════════════════════════════
# OUTDOOR SPORTS — only these check weather
# ═══════════════════════════════════════════════════════════════════
OUTDOOR_SPORTS = {
    'baseball_ncaa', 'baseball_mlb', 'soccer_epl', 'soccer_usa_mls',
    'soccer_germany_bundesliga', 'soccer_france_ligue_one',
    'soccer_italy_serie_a', 'soccer_spain_la_liga',
    'soccer_uefa_champs_league', 'soccer_mexico_ligamx',
}

# MLB teams with domed or retractable roof stadiums — weather irrelevant
ROOF_TEAMS = {
    'Arizona Diamondbacks',      # Chase Field — retractable roof
    'Tampa Bay Rays',            # Tropicana Field — dome
    'Houston Astros',            # Minute Maid Park — retractable roof
    'Seattle Mariners',          # T-Mobile Park — retractable roof
    'Texas Rangers',             # Globe Life Field — retractable roof
    'Toronto Blue Jays',         # Rogers Centre — retractable roof
    'Miami Marlins',             # loanDepot Park — retractable roof
    'Milwaukee Brewers',         # American Family Field — retractable roof
}

# Indoor sports — skip weather entirely
INDOOR_SPORTS = {
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl',
}


def _get_venue_coords(home_team, sport):
    """Get lat/lon for the home team's venue."""
    # Map sport to venue dict
    VENUE_MAP = {
        'baseball_mlb': MLB_VENUES,
        'soccer_usa_mls': MLS_VENUES,
        'soccer_epl': EPL_VENUES,
        'soccer_spain_la_liga': LA_LIGA_VENUES,
        'soccer_italy_serie_a': SERIE_A_VENUES,
        'soccer_germany_bundesliga': BUNDESLIGA_VENUES,
        'soccer_france_ligue_one': LIGUE_1_VENUES,
    }

    venues = VENUE_MAP.get(sport)
    if venues:
        # Try exact match first
        coords = venues.get(home_team)
        if coords:
            return coords
        # Fuzzy: try partial match (handles "Brighton and Hove Albion" vs "Brighton")
        home_lower = home_team.lower()
        for key, val in venues.items():
            if key.lower() in home_lower or home_lower in key.lower():
                return val

    # UCL: try all European venue dicts
    if 'champs' in sport or 'europa' in sport:
        for vdict in [EPL_VENUES, LA_LIGA_VENUES, SERIE_A_VENUES,
                      BUNDESLIGA_VENUES, LIGUE_1_VENUES]:
            for key, val in vdict.items():
                if key.lower() in home_team.lower() or home_team.lower() in key.lower():
                    return val

    return None


def _fetch_weather(lat, lon, game_time_utc=None):
    """
    Fetch weather from OpenWeatherMap.
    
    Uses forecast endpoint if game is in the future,
    current weather if game is within 2 hours.
    
    Returns dict: {temp_f, wind_mph, rain, description} or None
    """
    if not API_KEY:
        return None
    
    try:
        import urllib.request
        import json
        
        # Use forecast for future games
        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"
        
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        
        if not data.get('list'):
            return None
        
        # Find forecast closest to game time
        if game_time_utc:
            best = None
            best_diff = float('inf')
            for entry in data['list']:
                entry_time = datetime.fromtimestamp(entry['dt'], tz=timezone.utc)
                diff = abs((entry_time - game_time_utc).total_seconds())
                if diff < best_diff:
                    best_diff = diff
                    best = entry
        else:
            best = data['list'][0]
        
        if not best:
            return None
        
        wind_mph = best.get('wind', {}).get('speed', 0)
        temp_f = best.get('main', {}).get('temp', 70)
        rain_3h = best.get('rain', {}).get('3h', 0)
        description = best.get('weather', [{}])[0].get('description', '')
        
        return {
            'temp_f': round(temp_f),
            'wind_mph': round(wind_mph),
            'rain_mm': round(rain_3h, 1),
            'description': description,
        }
    
    except Exception as e:
        return None


def get_weather_adjustment(home_team, sport, commence=None):
    """
    Get weather-based adjustment for outdoor sports.
    
    Walters' weather rules (adapted):
    - Wind 15+ mph: total adjustment -1.5 (baseball), -0.3 (soccer)
    - Wind 20+ mph: total adjustment -3.0 (baseball), -0.5 (soccer)
    - Rain: total adjustment -1.0 (baseball), -0.3 (soccer)
    - Cold (<40°F): total adjustment -1.0 (baseball)
    - Wind 10+ mph: slight adjustment -0.5 (baseball)
    
    Returns: (adjustment, info_dict)
    """
    if sport in INDOOR_SPORTS:
        return 0.0, {}

    if sport not in OUTDOOR_SPORTS:
        return 0.0, {}

    # Skip domed/retractable roof MLB stadiums — no weather effect
    if home_team in ROOF_TEAMS:
        return 0.0, {}

    coords = _get_venue_coords(home_team, sport)
    if not coords:
        return 0.0, {}
    
    # Parse commence time
    game_time = None
    if commence:
        try:
            if isinstance(commence, str):
                game_time = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            else:
                game_time = commence
        except Exception:
            pass

    weather = _fetch_weather(coords[0], coords[1], game_time)
    if not weather:
        return 0.0, {}
    
    adj = 0.0
    reasons = {}
    
    wind = weather['wind_mph']
    temp = weather['temp_f']
    rain = weather['rain_mm']
    
    if 'baseball' in sport:
        # Baseball is most affected by weather
        if wind >= 20:
            adj -= 3.0
            reasons['strong_wind'] = f"Wind {wind}mph — heavy total depression"
        elif wind >= 15:
            adj -= 1.5
            reasons['wind'] = f"Wind {wind}mph — moderate total depression"
        elif wind >= 10:
            adj -= 0.5
            reasons['light_wind'] = f"Wind {wind}mph — slight total depression"
        
        if rain > 0:
            adj -= 1.0
            reasons['rain'] = f"Rain expected — suppresses scoring"
        
        if temp < 40:
            adj -= 1.0
            reasons['cold'] = f"Cold {temp}°F — ball doesn't carry"
    
    elif 'soccer' in sport:
        # Soccer less affected but still meaningful
        if wind >= 20:
            adj -= 0.5
            reasons['strong_wind'] = f"Wind {wind}mph — disrupts play"
        elif wind >= 15:
            adj -= 0.3
            reasons['wind'] = f"Wind {wind}mph — slight disruption"
        
        if rain > 2:
            adj -= 0.3
            reasons['rain'] = f"Heavy rain — slower play"
    
    return round(adj, 1), {**reasons, 'weather': weather}


# ═══════════════════════════════════════════════════════════════════
# CLI — Check weather for today's games
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    if not API_KEY:
        print("Set OPENWEATHER_API_KEY environment variable first:")
        print("  set OPENWEATHER_API_KEY=your_key_here")
        print("Get free key at: https://openweathermap.org/api")
    else:
        print(f"API key set. Testing with D.C. United venue...")
        weather = _fetch_weather(38.868, -77.013)
        if weather:
            print(f"  Weather: {weather}")
            adj, info = get_weather_adjustment('D.C. United', 'soccer_usa_mls')
            print(f"  Adjustment: {adj:+.1f}")
            print(f"  Info: {info}")
        else:
            print("  Failed to fetch weather. Check API key.")

import json
import requests
from dotmap import DotMap
from APICalls.config import get_config_from_json
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)

class TMDBRequests():

    def __init__(self):
        self.keys = get_config_from_json('..//Keys//keys.json')
        self.api_key = self.keys.tmdb_key.api_key
        self.language = 'en'
        self.debug = True

    def getPopularMovies(self, pages):
        popularMovies=pd.DataFrame()

        logging.info("Extracting popular movies")
        for page in range (1,pages+1):
            path = "https://api.themoviedb.org/3/movie/popular?api_key={}&language=en-US&page={}".format(self.api_key, page)
            response = requests.get(path)
            popularMovies_dict = response.json()
            popularMovies = popularMovies.append(popularMovies_dict['results'],ignore_index=True)
        popularMovies['tmdbCategory'] = 'popular'
        return popularMovies

    def getNowPlayingMovies(self, pages):
        nowPlayingMovies=pd.DataFrame()

        logging.info("Extracting now playing movies")
        for page in range (1,pages+1):
            path = "https://api.themoviedb.org/3/movie/now_playing?api_key={}&language=en-US&page={}".format(self.api_key, page)
            response = requests.get(path)
            nowPlayingMovies_dict = response.json()
            nowPlayingMovies = nowPlayingMovies.append(nowPlayingMovies_dict['results'],ignore_index=True)
        nowPlayingMovies['tmdbCategory'] = 'nowPlaying'
        return nowPlayingMovies

    def getTopRatedMovies(self, pages):
        topRatedMovies = pd.DataFrame()

        for page in range(1, pages + 1):
            path = "https://api.themoviedb.org/3/movie/top_rated?api_key={}&language=en-US&page={}".format(self.api_key,
                                                                                                        page)
            response = requests.get(path)
            topRatedMovies_dict = response.json()
            topRatedMovies = topRatedMovies.append(topRatedMovies_dict['results'], ignore_index=True)
        topRatedMovies['tmdbCategory'] = 'topRated'
        return topRatedMovies

    def getUpcomingMovies(self, pages):
        upcomingMovies = pd.DataFrame()

        logging.info("Extracting upcoming movies")
        for page in range(1, pages + 1):
            path = "https://api.themoviedb.org/3/movie/upcoming?api_key={}&language=en-US&page={}".format(self.api_key,
                                                                                                        page)
            response = requests.get(path)
            upcomingMovies_dict = response.json()
            upcomingMovies = upcomingMovies.append(upcomingMovies_dict['results'], ignore_index=True)
        upcomingMovies['tmdbCategory'] = 'upcoming'
        return upcomingMovies

    def getMovieData(self, movies):
        manualMovies = pd.DataFrame()

        logging.info("Extracting movies from manual list")
        for movie in movies:
            try:
                path= "https://api.themoviedb.org/3/search/movie?api_key={}&query={}".format(self.api_key, movie)
                response = requests.get(path)
                response_dict = response.json()
                manualMovies = manualMovies.append(response_dict['results'][0],ignore_index=True)
                manualMovies['tmdbCategory']='manual'
            except Exception as ex:
                logging.warning("Couldn't find {} in the database, check title name".format(movie))
        return manualMovies


    def getMovieGenres(self):
        path = "https://api.themoviedb.org/3/genre/movie/list?api_key={}&language=en-US".format(self.api_key)

        response = requests.get(path)
        movieGenres_dict = response.json()
        movieGenres = pd.DataFrame(movieGenres_dict['genres'])
        return movieGenres

    def getCombinedData(self,manualList=None):
        '''Gets data from TMDB for 4 categories - popular, top rated, upcoming and now playing and also checks with manual list
        Combines TMDB data into master allTitles dataframe and saves to file '''

        self.allTitles = pd.DataFrame()
        self.popularMovies = self.getPopularMovies(pages=2)
        self.topRatedMovies = self.getTopRatedMovies(pages=2)
        self.upcomingMovies = self.getUpcomingMovies(pages=2)
        self.nowPlayingMovies = self.getNowPlayingMovies(pages=2)
        self.manualMovies = self.getMovieData(manualList)

        self.allTitles = self.allTitles.append([self.popularMovies, self.topRatedMovies, self.upcomingMovies, self.nowPlayingMovies], ignore_index=True)
        logging.info("{} movies extracted from TMDB Curated Lists".format(self.allTitles.shape[0]))

        #Adding from manual data if not duplicates
        additions = 0
        for index,row in self.manualMovies.iterrows():
            if self.allTitles.empty:
                logging.info("Initialize all titles first")
            else:
                if row.id not in self.allTitles.id.values:
                    self.allTitles = self.allTitles.append(row,ignore_index=True)
                    additions +=1
                else:
                    logging.info("{} was excluded as its already present".format(row.title))
        logging.info("{} movies added manually with TMDB Data".format(additions))

        self.allTitles['content_type'] = "movie"
        self.allTitles.to_csv("..//Database//all_tmdb_movies.csv")
        logging.info("All titles data saved to file")

        return self.allTitles


if __name__ == '__main__':
    TMDB = TMDBRequests()
    manualMovies = pd.read_csv("..//Database//manual_movies.csv",header=None)
    TMDB.getCombinedData(manualMovies.values.tolist())
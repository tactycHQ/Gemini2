import json
import requests
import datetime
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

        try:
            self.manualMovies = pd.read_excel("..//Database//manual_movies.xlsx",header=0)
            self.manualIncl = self.manualMovies['inclusions'].dropna()
            self.manualExcl = self.manualMovies['exclusions'].dropna()

        except Exception as ex:
            print(ex)
            self.manualMovies =pd.DataFrame()
            logging.info("Couldn't find manual movie files. Creating new empty one")

        try:
            self.movie_titles = pd.read_csv("..//Database//movie_titles.csv",index_col=0, escapechar='/')
            logging.info("Found existing movie titles. Checking for manual and new TMDB pull data...")
        except Exception as ex:
            self.movie_titles = self.createCombinedTMDB()
            logging.info("Couldn't find movie_titles file. Initializing new file from latest TMDB pull")


    def getMovieGenres(self):
        path = "https://api.themoviedb.org/3/genre/movie/list?api_key={}&language=en-US".format(self.api_key)

        response = requests.get(path)
        movieGenres_dict = response.json()
        movieGenres = pd.DataFrame(movieGenres_dict['genres'])
        return movieGenres

    def getPopularMovies(self, pages):
        popularMovies=pd.DataFrame()

        logging.info("Extracting popular movies")
        for page in range (1,pages+1):
            path = "https://api.themoviedb.org/3/movie/popular?api_key={}&language=en-US&page={}".format(self.api_key, page)
            response = requests.get(path)
            popularMovies_dict = response.json()
            popularMovies = popularMovies.append(popularMovies_dict['results'],ignore_index=True)

        foreign_lang_idx = popularMovies[popularMovies['original_language'] != 'en'].index
        popularMovies.drop(foreign_lang_idx, inplace=True)

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

        foreign_language_idx = nowPlayingMovies[nowPlayingMovies['original_language'] != 'en'].index
        nowPlayingMovies.drop(foreign_language_idx, inplace=True)

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

        foreign_language_idx = topRatedMovies[topRatedMovies['original_language'] != 'en'].index
        topRatedMovies.drop(foreign_language_idx, inplace=True)

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

        foreign_lang_idx = upcomingMovies[upcomingMovies['original_language'] != 'en'].index
        upcomingMovies.drop(foreign_lang_idx,inplace=True)
        upcomingMovies['tmdbCategory'] = 'upcoming'

        return upcomingMovies

    def getUniqueTitles(self):
        '''Add titles from manual list while checking against duplicates'''
        logging.info("Getting unique titles from manual list")

        unique_from_tmdb = [title for title in self.manualIncl if title not in self.tmdbPull.title.values]
        unique_titles = [title for title in unique_from_tmdb if title not in self.movie_titles.title.values]
        logging.info("{} unique titles found in manual search".format(len(unique_titles)))

        return unique_titles

    def manualAdds(self):

        # get unique titles checking against current DB and against latest TMDB Pull
        unique_titles = self.getUniqueTitles()

        # get TMDB data for unique manual additions
        manualData = pd.DataFrame()
        for title in unique_titles:
            try:
                path= "https://api.themoviedb.org/3/search/movie?api_key={}&query={}".format(self.api_key, title)
                response = requests.get(path)
                response_dict = response.json()
                manualData = manualData.append(response_dict['results'][0],ignore_index=True)
                manualData['tmdbCategory']='manual'
                logging.info("Received metadata from TMDB for {}".format(title))
            except Exception as ex:
                logging.warning("Did not find {} on TMDB".format(title))
                print(ex)


        #add manual titles with TMDB data to existing DB
        additions = 0
        for index, row in manualData.iterrows():
            if self.movie_titles.empty:
                logging.info("Initialize all titles first")
            else:
                self.movie_titles = self.movie_titles.append(row, ignore_index=True).reset_index(drop=True)
                additions += 1
                logging.info("{} added to database from manual list".format(row.title))



        logging.info("{} movies added manually with TMDB Data".format(additions))

    def newTMDBAdds(self):
        '''Check against current database for duplicates'''
        try:
            logging.info("Checking for duplicates against current database")
            new_adds = 0

            for index, row in self.tmdbPull.iterrows():
                if row.title not in self.movie_titles['title'].values:
                    new_adds+=1
                    self.movie_titles = self.movie_titles.append(row,ignore_index=False).reset_index(drop=True)
                    logging.info("{} was not found in existing DB so adding it.".format(row.title))
            if new_adds == 0:
                logging.info("No new titles found in TMDB search")
            else:
                logging.info("Total of {} new titles found and added in TMDB search".format(new_adds))
            logging.info("Existing DB saved and updated with {} new titles and {} total titles".format(new_adds,self.movie_titles.shape[0]))
        except Exception as ex:
            print(ex)
            logging.info("No existing database was found. Created new file with {} titles".format(self.movie_titles.shape[0]))

    def createCombinedTMDB(self):
        self.tmdbPull = pd.DataFrame()
        popularMovies = self.getPopularMovies(pages=2)
        topRatedMovies = self.getTopRatedMovies(pages=2)
        upcomingMovies = self.getUpcomingMovies(pages=2)
        nowPlayingMovies = self.getNowPlayingMovies(pages=2)
        self.tmdbPull = self.tmdbPull.append([popularMovies,
                                              topRatedMovies,
                                              upcomingMovies,
                                              nowPlayingMovies],
                                             ignore_index=True)
        self.tmdbPull.reset_index(drop=True,inplace=True)

        logging.info("{} movies extracted from TMDB Curated Lists".format(self.tmdbPull.shape[0]))
        return self.tmdbPull

    def dropDuplicates(self):
        unique_df = self.movie_titles.drop_duplicates(subset=['title'], keep='first')
        self.movie_titles = unique_df.reset_index(drop=True)
        logging.info("{} remain after duplicates rationalization".format(self.movie_titles.shape[0]))

    def removeExclusions(self):
        df = self.movie_titles

        idx_drop = df[df['title'].isin(self.manualExcl)].index
        result_df = df.drop(idx_drop, inplace = False)

        self.movie_titles = result_df.reset_index(drop=True)
        logging.info("{} remain after exclusions".format(self.movie_titles.shape[0]))

    def updateMovieTitles(self,manualList=None):
        '''Gets data from TMDB for 4 categories - popular, top rated, upcoming and now playing and also checks with manual list
        Combines TMDB data into master allTitles dataframe and saves to file '''

        #Creates TMDB Pull list from latest search
        self.createCombinedTMDB()

        #Adding from manual data if not duplicates
        self.manualAdds()

        # Adding from manual data if not duplicates
        self.newTMDBAdds()

        # Remove duplicates
        self.dropDuplicates()

        #remove exclusions
        self.removeExclusions()

        self.movie_titles['content_type'] = "movie"
        self.movie_titles.to_csv("..//Database//movie_titles.csv")

if __name__ == '__main__':
    TMDB = TMDBRequests()
    TMDB.updateMovieTitles()



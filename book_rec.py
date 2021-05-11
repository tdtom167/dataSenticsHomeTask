import argparse
import boto3
import pandas as pd

from enum import Enum

# Constants
DATABASE_URL = 'http://localhost:8000'
RESOURCE_NAME = 'dynamodb'
ITEMS_DICT_KEY = 'Items'
TABLE_NAME = 'Books-ratings'


class ColumnNames(Enum):
    """Constants for the column names of the table Book-Ratings from DynamoDB"""

    ISBN = 'ISBN'
    BOOK_TITLE = 'Book-Title'
    BOOK_AUTHOR = 'Book-Author'
    YEAR_OF_PUB = 'Year-Of-Publication'
    PUBLISHER = 'Publisher'
    USER_ID = 'User-ID'
    BOOK_RATING = 'Book-Rating'
    LOCATION = 'Location'
    AGE = 'Age'


class DataLoader:
    """Class used for loading data from specified url"""

    def __init__(self, endpoint_url):
        self.__dynamodb = boto3.resource(RESOURCE_NAME, endpoint_url=endpoint_url)

    def get_all_data_from_table(self, table_name) -> pd.DataFrame:
        """Return pandas dataframe with all the data stored in table_name"""
        items_from_db = self.__dynamodb.Table(table_name).scan()

        return pd.DataFrame(items_from_db[ITEMS_DICT_KEY])


class BookRecomender:
    __book_data = []

    def __init__(self, book_data):
        self.__book_data = self._preproces_data(book_data)

    def _preproces_data(self, book_data) -> pd.DataFrame:
        """
            Preprocesses data
            makes all the columns lowercased and converts numeric columns to correct type
        """
        
        lowercased = book_data.apply(lambda x: x.str.lower() if(x.dtype == 'object') else x)
        columnsToConvertToNumeric = [ColumnNames.AGE.value, ColumnNames.YEAR_OF_PUB.value, ColumnNames.BOOK_RATING.value]
        lowercased[columnsToConvertToNumeric] = lowercased[columnsToConvertToNumeric].apply(lambda row:  pd.to_numeric(row, errors='ignore'))

        return lowercased

    def reccomend_books(self, book_title, count=10) -> pd.DataFrame:
        """
            Does a simple book recommendation, based on the title entered
        """

        # Find readers who also read the book
        book_title_readers_ids = self.__book_data[ColumnNames.USER_ID.value][self.__book_data[ColumnNames.BOOK_TITLE.value].str.contains(book_title.lower())].unique()

        # Find other books that the readers read
        books_of_title_readers = self.__book_data[(self.__book_data[ColumnNames.USER_ID.value].isin(book_title_readers_ids))]

        # Compute mean rating of this books
        book_title_with_ratings = books_of_title_readers[[ColumnNames.BOOK_TITLE.value, ColumnNames.BOOK_RATING.value]].groupby([ColumnNames.BOOK_TITLE.value]).mean()

        # Return first _count_ book sorted according to their mean rating
        return book_title_with_ratings.sort_values(ColumnNames.BOOK_RATING.value, ascending=False).head(count)


class Main:
    """Runs the book reccomendation engine"""

    def parse_args(self) -> argparse:
        parser = argparse.ArgumentParser(description='')
        parser.add_argument('book_name', metavar='Book name', type=str,
            help='A book name to which you want reccomendations')

        return parser.parse_args()

    def run(self):
        args = self.parse_args()
        print("Finding books similar to those that have " + args.book_name + " in name ;)")
        data_loader = DataLoader(DATABASE_URL)
        book_recomender = BookRecomender(data_loader.get_all_data_from_table(TABLE_NAME))
        print("The books I reccomend for you are: ")
        print(book_recomender.reccomend_books(args.book_name))
        print("The books are sorted according to their rating from users that also read the book you entered")

if __name__ == "__main__":
    Main().run()

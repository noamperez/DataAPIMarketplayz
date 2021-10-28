from flask import Flask
from flask_classful import FlaskView, route
from flask_restful import Resource, Api, reqparse, request
import pyodbc
import pandas as pd
import datetime
import ast

app = Flask(__name__)
api = Api(app)


def getConnection():
    """
    Generate the connection to DB

    Returns:

    pyodbc connection to DB.
    """
    server = 'noamperez.database.windows.net'
    database = 'MarketPlayz'
    username = 'noamperez'
    password = 'Nope2607'
    driver = '{ODBC Driver 17 for SQL Server}'
    return pyodbc.connect(
        f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}')


class Game(FlaskView):
    """
    Manages the API functions for Games Table in DB.

    Parameters of game :

    ID - the ID of the game

    Title - the game name

    Platform - Xbox, Sony, PC.
    """
    def isGameExist(self, gameID):
        """
        Checks if there is a game in the DB with such ID.

        Args:

        `gameID`: The ID of the game.

        Returns:

        True if exists or False otherwise.
        """
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[Games] WHERE ID = {gameID}", getConnection())
        return not data.empty

    @route('/', methods=['GET'])
    def get(self):
        """
        route : /Games

        Get All games from DB.

        Returns:

        List of games(json)
        """
        data = pd.read_sql_query("SELECT * FROM [dbo].[Games]", getConnection())
        data = data.to_dict()
        return {'data': data}, 200

    @route('/getByID', methods=['GET'])
    def getByID(self):
        """
        route : /Games/getByID

        Gets a game by ID.

        Args:

        `ID` (api argument): The ID of the game.

        Returns:

        The game parameters if exists, error message otherwise.
        """
        if 'ID' in request.args:
            gameID = request.args['ID']
            try:
                data = pd.read_sql_query(f"SELECT * FROM [dbo].[Games] WHERE ID = {gameID} ", getConnection())
                if data.empty:
                    return "There is no game with this ID.", 404
                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{gameID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/', methods=['POST'])
    def post(self):
        """
        route : /Games/

        Adds a game to the DB.

        Args:

        `ID` (api argument): The ID of the game.
        `Title` (api argument): The Title of the game.
        `Platform` (api argument): The Platform of the game.

        Returns:

        Approval message if succeeded, error message otherwise.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ID', required=True)
        parser.add_argument('Title', required=True)
        parser.add_argument('Platform', required=True)
        args = parser.parse_args()

        try:
            if self.isGameExist(args['ID']):
                return f"'Game with ID = {args['ID']}' already exists.", 401
        except:
            return f"'{args['ID']}' is invalid ID.", 400

        query = f"INSERT INTO [dbo].[Games] VALUES({args['ID']}, '{args['Title']}', '{args['Platform']}')"
        connection = getConnection()
        connection.cursor().execute(query)
        connection.commit()
        return "The Game has been successfully added.", 200

    @route('/', methods=['PUT'])
    def put(self):
        """
        route : /Games/

        Updates a game from the DB.

        Args:

        `ID` (api argument): The ID of the game.
        `Title` (api argument): The Title of the game.
        `Platform` (api argument): The Platform of the game.

        Returns:

        Approval message if succeeded, error message otherwise.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ID', required=True)
        parser.add_argument('Title', required=True)
        parser.add_argument('Platform', required=True)
        args = parser.parse_args()

        try:
            if not self.isGameExist(args['ID']):
                return f"Game with ID = {args['ID']} is not found.", 404
        except:
            return f"'{args['ID']}' is invalid ID.", 400

        query = f"UPDATE [dbo].[Games] SET Title = '{args['Title']}', Platform = '{args['Platform']}' WHERE ID = {args['ID']}"
        connection = getConnection()
        connection.cursor().execute(query)
        connection.commit()
        return "The Game has been successfully updated.", 200

    @route('/', methods=['DELETE'])
    def delete(self):
        """
        route : /Games/

        Deletes a game from the DB.

        Args:

        `ID` (api argument): The ID of the game.

        Returns:

        Approval message if succeeded, error message otherwise.
        """
        if 'ID' in request.args:
            gameID = request.args['ID']
            try:
                if not self.isGameExist(gameID):
                    return "There is no game with this ID.", 404

                query = f"DELETE [dbo].[Games] WHERE ID = {gameID}"
                connection = getConnection()
                connection.cursor().execute(query)
                connection.commit()
                return "The Game has been successfully deleted.", 200
            except:
                return f"'{gameID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400


class Player(FlaskView):
    """
    Manages the API functions for Players Table in DB.

    Parameters of Player :

    ID - the ID of the player

    games - the games the player plays

    Created_date - the date the player was created
    """
    def isPlayerExist(self, playerID):
        """
        Checks if there is a player in the DB with such ID.

        Args:

        `playerID`: The ID of the player.

        Returns:

        True if exists or False otherwise.
        """
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[Players] WHERE ID = {playerID}", getConnection())
        return not data.empty

    def isGamePlayedByPlayer(self, playerID, gameID):
        """
        Checks if a game was played by a player.

        Args:

        `playerID`: The ID of the player.

        `gameID`: The ID of the game.

        Returns:

        True if was played or False otherwise.
        """
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[GamesOfPlayers] WHERE"
                                 f" playerID = {playerID} and gameID = {gameID}", getConnection())
        return not data.empty

    @route('/', methods=['GET'])
    def get(self):
        """
        route : /Players/

        Get All players from DB.

        Returns:

        List of players(json)
        """
        data = pd.read_sql_query("SELECT * FROM [dbo].[Players]", getConnection())
        games = []
        for playerID in data["ID"]:
            games.insert(0, pd.read_sql_query(f"SELECT Games.Title FROM Games \nINNER JOIN GamesOfPlayers ON "
                                              f"Games.ID=GamesOfPlayers.GameID WHERE PlayerID = {playerID}",
                                              getConnection()).to_dict())

        data["games"] = games
        data = data.to_dict()
        return {'data': data}, 200

    @route('/getByID', methods=['GET'])
    def getByID(self):
        """
        route : /Players/getByID

         Gets a player by ID.

         Args:

         `ID` (api argument): The ID of the player.

         Returns:

         The player parameters if exists, error message otherwise.
         """
        if 'ID' in request.args:
            playerID = request.args['ID']
            try:
                data = pd.read_sql_query(f"SELECT * FROM [dbo].[Players] WHERE ID = {playerID} ", getConnection())
                if data.empty:
                    return "There is no player with this ID.", 404
                data["games"] = pd.read_sql_query(f"SELECT Games.Title FROM Games \nINNER JOIN GamesOfPlayers ON "
                                                  f"Games.ID=GamesOfPlayers.GameID WHERE PlayerID = {playerID}",
                                                  getConnection()).to_dict()
                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{playerID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/', methods=['POST'])
    def post(self):
        """
          route : /Players/

          Adds a player to the DB.

          Args:

          `ID` (api argument): The ID of the player.
          `Games` (api argument): The Games of the player.

          Returns:

          Approval message if succeeded, error message otherwise.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ID', required=True)
        parser.add_argument('Games', required=False)
        args = parser.parse_args()

        try:
            if self.isPlayerExist(args['ID']):
                return f"'Player with ID = {args['ID']}' already exists.", 401
        except:
            return f"'{args['ID']}' is invalid ID.", 400
        # Add player
        today = datetime.date.today().strftime("%Y-%m-%d")
        query = f"INSERT INTO [dbo].[Players] VALUES({args['ID']}, '{today}')"
        connection = getConnection()
        connection.cursor().execute(query)
        connection.commit()

        # Add games
        try:
            if args['Games']:
                args['Games'] = ast.literal_eval(args['Games'])
                for gameID in args['Games']:
                    query = f"INSERT INTO [dbo].[GamesOfPlayers] VALUES({args['ID']}, '{gameID}')"
                    connection.cursor().execute(query)
                    connection.commit()
            return "The Player has been successfully added.", 200
        except:
            return "The games are invalid but the player has been successfully added."

    @route('/', methods=['PUT'])
    def put(self):
        """
          route : /Players/
          Updates a player from the DB.

          Args:

          `ID` (api argument): The ID of the player.
          `Gamed` (api argument): The Title of the player.

          Returns:

          Approval message if succeeded, error message otherwise.
          """
        parser = reqparse.RequestParser()
        parser.add_argument('ID', required=True)
        parser.add_argument('Games', required=True)
        args = parser.parse_args()

        try:
            if not self.isPlayerExist(args['ID']):
                return f"Player with ID = {args['ID']} is not found.", 404
        except:
            return f"'{args['ID']}' is invalid ID.", 400

        connection = getConnection()
        try:
            if args['Games']:
                args['Games'] = ast.literal_eval(args['Games'])
                for gameID in args['Games']:
                    if not self.isGamePlayedByPlayer(args['ID'], gameID):
                        query = f"INSERT INTO [dbo].[GamesOfPlayers] VALUES({args['ID']}, '{gameID}')"
                        connection.cursor().execute(query)
                        connection.commit()
            return "The Player has been successfully updated.", 200
        except:
            return "The games are invalid or one of them is already in the list.", 400

    @route('/', methods=['DELETE'])
    def delete(self):
        """
        route : /Players/

        Deletes a player from the DB.

        Args:

        `ID` (api argument): The ID of the player.

        Returns:

        Approval message if succeeded, error message otherwise.
        """
        if 'ID' in request.args:
            ID = request.args['ID']
            try:
                if not self.isPlayerExist(ID):
                    return "There is no player with this ID.", 404

                connection = getConnection()
                query = f"DELETE [dbo].[GamesOfPlayers] WHERE PlayerID = {ID}"
                connection.cursor().execute(query)
                connection.commit()

                query = f"DELETE [dbo].[Players] WHERE ID = {ID}"
                connection.cursor().execute(query)
                connection.commit()
                return "The Player has been successfully deleted.", 200
            except:
                return f"'{ID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/getAllGamesOfPlayer', methods=['GET'])
    def getAllGamesOfPlayer(self):
        """
        route : /Players/getAllGamesOfPlayer

        Get the games that player has played from the DB.

        Args:

        `ID` (api argument): The ID of the player.

        Returns:

        List of games.
        """
        if 'ID' in request.args:
            playerID = request.args['ID']
            try:
                if not self.isPlayerExist(playerID):
                    return "There is no player with this ID.", 404
                data = pd.read_sql_query(f"SELECT Games.Title FROM Games \nINNER JOIN GamesOfPlayers ON "
                                         f"Games.ID=GamesOfPlayers.GameID WHERE PlayerID = {playerID}",
                                         getConnection()).to_dict()

                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{playerID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/getAllPlayersByDate', methods=['GET'])
    def getAllPlayersByDate(self):
        """
        route : /Players/getAllPlayersByDate

        Get the players that was created at a specific date from the DB.

        Args:

        `Date` (api argument): The Date the players was created at.

        Returns:

        List of players.
        """
        if 'Date' in request.args:
            date = request.args['Date']
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
            date = date.date()
            try:
                query = f"SELECT * FROM [dbo].[Players]"
                data = pd.read_sql_query(query, con=getConnection())
                data = data.loc[data["Created_date"] == date]
                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{date}' is invalid.", 400
        else:
            return "Error: No Date field provided.", 400


class Observer:
    """
    Observe for events
    """
    _observers = []

    def __init__(self):
        self._observers.append(self)
        self._observed_events = []

    def observe(self, event_name, callback_fn):
        """
        Add event that was observed to the list of observed events.

        :param event_name:
        :param callback_fn:
        """
        self._observed_events.append({'event_name': event_name, 'callback_fn': callback_fn})


class Event:
    """
    Generic events that we want to observe.
    """
    def __init__(self, event_name, *callback_args):
        for observer in Observer._observers:
            for observable in observer._observed_events:
                if observable['event_name'] == event_name:
                    observable['callback_fn'](*callback_args)


class Subscribers(Observer):
    """
    Specific event that we want to observe - new subscribers for a gamer.
    """
    def __init__(self):
        Observer.__init__(self)

    def new_subscribe(self, player, subscriber_nickname):
        """
        Add to log file that a gamer has new subscriber and send a message
        :param player:
        :param subscriber_nickname:

        Returns a message to the gamer.
        """
        log_path = 'new_subscribers_log.txt'
        with open(log_path, 'w') as f:
            f.write(f"{subscriber_nickname} has just subscribed {player}. time : {datetime.datetime.today()}")
        f.close()
        return f"{subscriber_nickname} has just subscribed you.", player


if __name__ == '__main__':
    Game.register(app, route_base='/Games')
    Player.register(app, route_base='/Players')
    subscribers = Subscribers()
    subscribers.observe('new subscriber', subscribers.new_subscribe)
    app.run()  # run our Flask app

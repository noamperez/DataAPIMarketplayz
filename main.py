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
    server = 'noamperez.database.windows.net'
    database = 'MarketPlayz'
    username = 'noamperez'
    password = 'Nope2607'
    driver = '{ODBC Driver 17 for SQL Server}'
    return pyodbc.connect(f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}')


class Game(FlaskView):

    def isGameExist(self, gameID):
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[Games] WHERE ID = {gameID}", getConnection())
        return not data.empty

    @route('/', methods=['GET'])
    def get(self):
        data = pd.read_sql_query("SELECT * FROM [dbo].[Games]", getConnection())
        data = data.to_dict()
        return {'data': data}, 200

    @route('/getByID', methods=['GET'])
    def getByID(self):
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

    def isPlayerExist(self, playerID):
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[Players] WHERE ID = {playerID}", getConnection())
        return not data.empty

    def isGamePlayedByPlayer(self, playerID, gameID):
        data = pd.read_sql_query(f"SELECT * FROM [dbo].[GamesOfPlayers] WHERE"
                                 f" playerID = {playerID} and gameID = {gameID}", getConnection())
        return not data.empty

    @route('/', methods=['GET'])
    def get(self):
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
        if 'ID' in request.args:
            playerID = request.args['ID']
            try:
                data = pd.read_sql_query(f"SELECT * FROM [dbo].[Players] WHERE ID = {playerID} ", getConnection())
                if data.empty:
                    return "There is no player with this ID.", 404
                data["games"] = pd.read_sql_query(f"SELECT Games.Title FROM Games \nINNER JOIN GamesOfPlayers ON "
                                     f"Games.ID=GamesOfPlayers.GameID WHERE PlayerID = {playerID}", getConnection()).to_dict()
                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{playerID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/', methods=['POST'])
    def post(self):
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
        if 'ID' in request.args:
            playerID = request.args['ID']
            try:
                if not self.isPlayerExist(playerID):
                    return "There is no player with this ID.", 404
                data = pd.read_sql_query(f"SELECT Games.Title FROM Games \nINNER JOIN GamesOfPlayers ON "
                                         f"Games.ID=GamesOfPlayers.GameID WHERE PlayerID = {playerID}", getConnection()).to_dict()

                data = data.to_dict()
                return {'data': data}, 200
            except:
                return f"'{playerID}' is invalid ID.", 400
        else:
            return "Error: No ID field provided.", 400

    @route('/getAllPlayersByDate', methods=['GET'])
    def getAllPlayersByDate(self):
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


if __name__ == '__main__':
    Game.register(app, route_base='/Games')
    Player.register(app, route_base='/Players')
    app.run()  # run our Flask app
